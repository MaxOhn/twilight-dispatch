use crate::{config::CONFIG, models::ApiResult};

use bathbot_cache::Cache;
use futures_util::Stream;
use std::{
    collections::HashMap, fmt::Debug, future::Future, pin::Pin, slice, sync::Arc, time::Duration,
};
use time::{Format, OffsetDateTime};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot::{self, Sender},
    },
    time::sleep,
};
use tracing::warn;
use twilight_gateway::{
    cluster::ShardScheme, queue::Queue, shard::ResumeSession, Cluster, Event, EventTypeFlags,
    Intents,
};
use twilight_model::{
    channel::embed::Embed,
    gateway::{payload::update_presence::UpdatePresencePayload, presence::Activity},
    id::ChannelId,
};

#[derive(Clone, Debug)]
pub struct LocalQueue(UnboundedSender<Sender<()>>);

impl LocalQueue {
    pub fn new(duration: Duration) -> Self {
        let (tx, rx) = unbounded_channel();
        tokio::spawn(waiter(rx, duration));

        Self(tx)
    }
}

impl Queue for LocalQueue {
    fn request(&'_ self, [_, _]: [u64; 2]) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();

            if let Err(err) = self.0.clone().send(tx) {
                warn!("skipping, send failed: {:?}", err);
                return;
            }

            let _ = rx.await;
        })
    }
}

#[derive(Debug)]
pub struct LargeBotQueue(Vec<UnboundedSender<Sender<()>>>);

impl LargeBotQueue {
    pub fn new(buckets: usize, duration: Duration) -> Self {
        let mut queues = Vec::with_capacity(buckets);
        for _ in 0..buckets {
            let (tx, rx) = unbounded_channel();
            tokio::spawn(waiter(rx, duration));
            queues.push(tx)
        }

        Self(queues)
    }
}

impl Queue for LargeBotQueue {
    fn request(&'_ self, shard_id: [u64; 2]) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        #[allow(clippy::cast_possible_truncation)]
        let bucket = (shard_id[0] % (self.0.len() as u64)) as usize;
        let (tx, rx) = oneshot::channel();

        Box::pin(async move {
            if let Err(err) = self.0[bucket].clone().send(tx) {
                warn!("skipping, send failed: {:?}", err);
                return;
            }

            let _ = rx.await;
        })
    }
}

async fn waiter(mut rx: UnboundedReceiver<Sender<()>>, duration: Duration) {
    while let Some(req) = rx.recv().await {
        if let Err(err) = req.send(()) {
            warn!("skipping, send failed: {:?}", err);
        }
        sleep(duration).await;
    }
}

pub async fn get_clusters(
    resumes: HashMap<u64, ResumeSession>,
    queue: Arc<dyn Queue>,
) -> ApiResult<(
    Vec<Cluster>,
    Vec<impl Stream<Item = (u64, Event)> + Send + Sync + Unpin + 'static>,
)> {
    let shards = get_shards();
    let base = shards / CONFIG.clusters;
    let extra = shards % CONFIG.clusters;

    let mut clusters = Vec::with_capacity(CONFIG.clusters as usize);
    let mut events = Vec::with_capacity(CONFIG.clusters as usize);
    let mut last_index = CONFIG.shards_start;

    for i in 0..CONFIG.clusters {
        let index = if i < extra {
            last_index + base
        } else {
            last_index + base - 1
        };

        let (cluster, event) = Cluster::builder(
            CONFIG.bot_token.clone(),
            Intents::from_bits(CONFIG.intents).unwrap(),
        )
        .gateway_url(Some("wss://gateway.discord.gg".to_owned()))
        .shard_scheme(ShardScheme::Range {
            from: last_index,
            to: index,
            total: CONFIG.shards_total,
        })
        .queue(queue.clone())
        .presence(
            UpdatePresencePayload::new(
                vec![Activity {
                    application_id: None,
                    assets: None,
                    buttons: Vec::new(),
                    created_at: None,
                    details: None,
                    emoji: None,
                    flags: None,
                    id: None,
                    instance: None,
                    kind: CONFIG.activity_type,
                    name: CONFIG.activity_name.clone(),
                    party: None,
                    secrets: None,
                    state: None,
                    timestamps: None,
                    url: None,
                }],
                false,
                None,
                CONFIG.status,
            )
            .unwrap(),
        )
        .large_threshold(CONFIG.large_threshold)?
        .resume_sessions(resumes.clone())
        .event_types(get_event_flags())
        .build()
        .await?;

        clusters.push(cluster);
        events.push(event);

        last_index = index + 1;
    }

    Ok((clusters, events))
}

pub fn get_queue() -> Arc<dyn Queue> {
    let concurrency = CONFIG.shards_concurrency as usize;
    let wait = Duration::from_secs(CONFIG.shards_wait);
    if concurrency == 1 {
        Arc::new(LocalQueue::new(wait))
    } else {
        Arc::new(LargeBotQueue::new(concurrency, wait))
    }
}

pub async fn get_resume_sessions(cache: &Cache) -> ApiResult<HashMap<u64, ResumeSession>> {
    let shards = cache.shards().await?.unwrap_or_default();

    if shards != CONFIG.shards_total || !CONFIG.resume {
        return Ok(HashMap::new());
    }

    let sessions = cache
        .sessions()
        .await?
        .unwrap_or_default()
        .into_iter()
        .map(|(k, v)| {
            let session = ResumeSession {
                session_id: v.session_id,
                sequence: v.sequence,
            };

            (k.parse().unwrap(), session)
        });

    Ok(sessions.collect())
}

// TODO
pub fn get_event_flags() -> EventTypeFlags {
    EventTypeFlags::GATEWAY_HELLO
        | EventTypeFlags::GATEWAY_INVALIDATE_SESSION
        | EventTypeFlags::GATEWAY_RECONNECT
        | EventTypeFlags::READY
        | EventTypeFlags::RESUMED
        | EventTypeFlags::SHARD_CONNECTED
        | EventTypeFlags::SHARD_CONNECTING
        | EventTypeFlags::SHARD_DISCONNECTED
        | EventTypeFlags::SHARD_IDENTIFYING
        | EventTypeFlags::SHARD_PAYLOAD
        | EventTypeFlags::SHARD_RECONNECTING
        | EventTypeFlags::SHARD_RESUMING
        | EventTypeFlags::CHANNEL_CREATE
        | EventTypeFlags::CHANNEL_DELETE
        | EventTypeFlags::CHANNEL_UPDATE
        | EventTypeFlags::GUILD_CREATE
        | EventTypeFlags::GUILD_DELETE
        | EventTypeFlags::GUILD_UPDATE
        | EventTypeFlags::INTERACTION_CREATE
        | EventTypeFlags::MEMBER_ADD
        | EventTypeFlags::MEMBER_REMOVE
        | EventTypeFlags::MEMBER_UPDATE
        | EventTypeFlags::MEMBER_CHUNK
        | EventTypeFlags::MESSAGE_CREATE
        | EventTypeFlags::REACTION_ADD
        | EventTypeFlags::REACTION_REMOVE
        | EventTypeFlags::ROLE_CREATE
        | EventTypeFlags::ROLE_DELETE
        | EventTypeFlags::ROLE_UPDATE
        | EventTypeFlags::THREAD_CREATE
        | EventTypeFlags::THREAD_DELETE
        | EventTypeFlags::THREAD_UPDATE
        | EventTypeFlags::UNAVAILABLE_GUILD
        | EventTypeFlags::USER_UPDATE
}

pub fn log_discord(cluster: &Cluster, color: usize, message: impl Into<String>) {
    let client = cluster.config().http_client().clone();
    let message = message.into();

    tokio::spawn(async move {
        let embed = Embed {
            author: None,
            color: Some(color as u32),
            description: None,
            fields: vec![],
            footer: None,
            image: None,
            kind: "rich".to_owned(),
            provider: None,
            thumbnail: None,
            timestamp: Some(OffsetDateTime::now_utc().format(Format::Rfc3339)),
            title: Some(message),
            url: None,
            video: None,
        };

        let message = client
            .create_message(ChannelId(CONFIG.log_channel))
            .embeds(slice::from_ref(&embed));

        if let Ok(message) = message {
            if let Err(err) = message.exec().await {
                warn!("Failed to post message to Discord: {:?}", err)
            }
        }
    });
}

pub fn get_shards() -> u64 {
    CONFIG.shards_end - CONFIG.shards_start + 1
}
