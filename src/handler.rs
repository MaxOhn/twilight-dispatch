use crate::{
    config::CONFIG,
    constants::{CONNECT_COLOR, DISCONNECT_COLOR, EXCHANGE, QUEUE_SEND, READY_COLOR, RESUME_COLOR},
    metrics::{GATEWAY_EVENTS, SHARD_EVENTS},
    models::{DeliveryInfo, DeliveryOpcode, PayloadInfo},
    utils::log_discord,
};

use bathbot_cache::Cache;
use futures_util::{Stream, StreamExt};
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions},
    types::FieldTable,
    BasicProperties, Channel,
};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::mpsc,
    time::{interval, timeout, MissedTickBehavior},
};
use tracing::{info, warn};
use twilight_gateway::{Cluster, Event};
use twilight_model::gateway::payload::RequestGuildMembers;

pub async fn outgoing<E>(cache: Arc<Cache>, cluster: Cluster, channel: Channel, mut events: E)
where
    E: Stream<Item = (u64, Event)> + Send + Sync + Unpin + 'static,
{
    let update_time = Duration::from_millis(CONFIG.cache_update_deadline_ms);
    let shard_strings: Vec<String> = (0..CONFIG.shards_total).map(|x| x.to_string()).collect();

    let (tx, mut rx) = mpsc::channel(10_000);
    let member_cluster = cluster.clone();

    tokio::spawn(async move {
        let mut interval = interval(Duration::from_millis(CONFIG.member_request_delay_ms));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        while let Some((guild, shard)) = rx.recv().await {
            interval.tick().await;
            let req = RequestGuildMembers::builder(guild).query("", None);

            if let Err(why) = member_cluster.command(shard, &req).await {
                warn!("Failed to request members for guild {}: {}", guild, why);
            }
        }
    });

    while let Some((shard, event)) = events.next().await {
        match timeout(update_time, cache.update(&event)).await {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                warn!(
                    "[Shard {}] Failed to update state for {:?}: {:?}",
                    shard,
                    event.kind(),
                    err
                );
            }
            Err(_) => {
                warn!(
                    "[Shard {}] Timed out while updating state ({:?})",
                    shard,
                    event.kind()
                );
            }
        }

        match event {
            Event::GatewayHello(data) => {
                info!("[Shard {}] Hello (heartbeat interval: {})", shard, data);
            }
            Event::GatewayInvalidateSession(data) => {
                info!("[Shard {}] Invalid Session (resumable: {})", shard, data);
            }
            Event::GuildCreate(e) => {
                if let Err(why) = tx.send((e.id, shard)).await {
                    warn!("Failed to forward member request: {}", why);
                }
            }
            Event::Ready(data) => {
                info!("[Shard {}] Ready (session: {})", shard, data.session_id);
                log_discord(&cluster, READY_COLOR, format!("[Shard {}] Ready", shard));
                SHARD_EVENTS.with_label_values(&["Ready"]).inc();
            }
            Event::Resumed => {
                if let Some(Ok(info)) = cluster.shard(shard as u64).map(|s| s.info()) {
                    info!(
                        "[Shard {}] Resumed (session: {})",
                        shard,
                        info.session_id().unwrap_or_default()
                    );
                } else {
                    info!("[Shard {}] Resumed", shard);
                }
                log_discord(&cluster, RESUME_COLOR, format!("[Shard {}] Resumed", shard));
                SHARD_EVENTS.with_label_values(&["Resumed"]).inc();
            }
            Event::ShardConnected(_) => {
                info!("[Shard {}] Connected", shard);
                log_discord(
                    &cluster,
                    CONNECT_COLOR,
                    format!("[Shard {}] Connected", shard),
                );
                SHARD_EVENTS.with_label_values(&["Connected"]).inc();
            }
            Event::ShardConnecting(data) => {
                info!("[Shard {}] Connecting (url: {})", shard, data.gateway);
                SHARD_EVENTS.with_label_values(&["Connecting"]).inc();
            }
            Event::ShardDisconnected(data) => {
                if let Some(code) = data.code {
                    let reason = data.reason.unwrap_or_default();
                    if !reason.is_empty() {
                        info!(
                            "[Shard {}] Disconnected (code: {}, reason: {})",
                            shard, code, reason
                        );
                    } else {
                        info!("[Shard {}] Disconnected (code: {})", shard, code);
                    }
                } else {
                    info!("[Shard {}] Disconnected", shard);
                }
                log_discord(
                    &cluster,
                    DISCONNECT_COLOR,
                    format!("[Shard {}] Disconnected", shard),
                );
                SHARD_EVENTS.with_label_values(&["Disconnected"]).inc();
            }
            Event::ShardIdentifying(_) => {
                info!("[Shard {}] Identifying", shard);
                SHARD_EVENTS.with_label_values(&["Identifying"]).inc();
            }
            Event::ShardReconnecting(_) => {
                info!("[Shard {}] Reconnecting", shard);
                SHARD_EVENTS.with_label_values(&["Reconnecting"]).inc();
            }
            Event::ShardResuming(data) => {
                info!("[Shard {}] Resuming (sequence: {})", shard, data.seq);
                SHARD_EVENTS.with_label_values(&["Resuming"]).inc();
            }
            Event::ShardPayload(data) => {
                match serde_json::from_slice::<PayloadInfo>(data.bytes.as_slice()) {
                    Ok(payload) => {
                        if let Some(kind) = payload.t.as_deref() {
                            GATEWAY_EVENTS
                                .with_label_values(&[kind, shard_strings[shard as usize].as_str()])
                                .inc();

                            match serde_json::to_vec(&payload) {
                                Ok(payload) => {
                                    let result = channel
                                        .basic_publish(
                                            EXCHANGE,
                                            kind,
                                            BasicPublishOptions::default(),
                                            payload,
                                            BasicProperties::default(),
                                        )
                                        .await;

                                    if let Err(err) = result {
                                        warn!(
                                            "[Shard {}] Failed to publish event: {:?}",
                                            shard, err
                                        );
                                    }
                                }
                                Err(err) => {
                                    warn!(
                                        "[Shard {}] Failed to serialize payload: {:?}",
                                        shard, err
                                    );
                                }
                            }
                        }
                    }
                    Err(err) => {
                        warn!(
                            "[Shard {}] Could not decode payload: {:?}\n{:?}",
                            shard, err, data.bytes
                        );
                    }
                }
            }
            _ => {}
        }
    }
}

pub async fn incoming(clusters: &[Cluster], channel: &Channel) {
    let mut consumer = match channel
        .basic_consume(
            QUEUE_SEND,
            "",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
    {
        Ok(channel) => channel,
        Err(err) => {
            warn!("Failed to consume delivery channel: {:?}", err);
            return;
        }
    };

    while let Some(message) = consumer.next().await {
        match message {
            Ok((channel, delivery)) => {
                let _ = channel
                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                    .await;
                match serde_json::from_slice::<DeliveryInfo>(delivery.data.as_slice()) {
                    Ok(payload) => {
                        let cluster = clusters
                            .iter()
                            .find(|cluster| cluster.shard(payload.shard).is_some());
                        if let Some(cluster) = cluster {
                            match payload.op {
                                DeliveryOpcode::Send => {
                                    if let Err(err) = cluster
                                        .command(payload.shard, &payload.data.unwrap_or_default())
                                        .await
                                    {
                                        warn!("Failed to send gateway command: {:?}", err);
                                    }
                                }
                                DeliveryOpcode::Reconnect => {
                                    info!("Shutting down shard {}", payload.shard);
                                    cluster.shard(payload.shard).unwrap().shutdown();
                                }
                            }
                        } else {
                            warn!("Delivery received for invalid shard: {}", payload.shard)
                        }
                    }
                    Err(err) => {
                        warn!("Failed to deserialize payload: {:?}", err);
                    }
                }
            }
            Err(err) => {
                warn!("Failed to consume delivery: {:?}", err);
            }
        }
    }
}
