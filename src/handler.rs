use crate::{
    cache,
    config::CONFIG,
    constants::{CONNECT_COLOR, DISCONNECT_COLOR, EXCHANGE, QUEUE_SEND, READY_COLOR, RESUME_COLOR},
    metrics::{GATEWAY_EVENTS, SHARD_EVENTS},
    models::{DeliveryInfo, DeliveryOpcode, PayloadInfo},
    utils::log_discord,
};

use futures_util::{Stream, StreamExt};
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions},
    types::FieldTable,
    BasicProperties, Channel,
};
use std::time::Duration;
use tokio::time::timeout;
use tracing::{info, warn};
use twilight_gateway::{Cluster, Event};

pub async fn outgoing(
    conn: &mut redis::aio::Connection,
    cluster: &Cluster,
    channel: &lapin::Channel,
    mut events: impl Stream<Item = (u64, Event)> + Send + Sync + Unpin + 'static,
) {
    let shard_strings: Vec<String> = (0..CONFIG.shards_total).map(|x| x.to_string()).collect();

    while let Some((shard, event)) = events.next().await {
        let shard = shard as usize;

        let update_fut = cache::update(conn, &event);

        match timeout(Duration::from_millis(10_000), update_fut).await {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                warn!("[Shard {}] Failed to update state: {:?}", shard, err);
            }
            Err(_) => {
                warn!("[Shard {}] Timed out while updating state", shard);
            }
        }

        match event {
            Event::GatewayHello(data) => {
                info!("[Shard {}] Hello (heartbeat interval: {})", shard, data);
            }
            Event::GatewayInvalidateSession(data) => {
                info!("[Shard {}] Invalid Session (resumable: {})", shard, data);
            }
            Event::Ready(data) => {
                info!("[Shard {}] Ready (session: {})", shard, data.session_id);
                log_discord(cluster, READY_COLOR, format!("[Shard {}] Ready", shard));
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
                log_discord(cluster, RESUME_COLOR, format!("[Shard {}] Resumed", shard));
                SHARD_EVENTS.with_label_values(&["Resumed"]).inc();
            }
            Event::ShardConnected(_) => {
                info!("[Shard {}] Connected", shard);
                log_discord(
                    cluster,
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
                    cluster,
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
            Event::ShardPayload(mut data) => {
                match simd_json::from_slice::<PayloadInfo>(data.bytes.as_mut_slice()) {
                    Ok(payload) => {
                        if let Some(kind) = payload.t.as_deref() {
                            GATEWAY_EVENTS
                                .with_label_values(&[kind, shard_strings[shard as usize].as_str()])
                                .inc();

                            match simd_json::to_vec(&payload) {
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
                        warn!("[Shard {}] Could not decode payload: {:?}", shard, err);
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
            Ok((channel, mut delivery)) => {
                let _ = channel
                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                    .await;
                match simd_json::from_slice::<DeliveryInfo>(delivery.data.as_mut_slice()) {
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
