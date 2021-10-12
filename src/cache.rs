use crate::{
    config::CONFIG,
    constants::{
        channel_key, guild_key, member_key, role_key, user_key, BOT_USER_KEY, CACHE_DUMP_INTERVAL,
        CHANNEL_KEY, GUILD_KEY, KEYS_SUFFIX, MESSAGE_KEY, SESSIONS_KEY, STATUSES_KEY,
    },
    models::{
        ApiError, ApiResult, CachedMember, FormattedDateTime, GuildItem, SessionInfo, StatusInfo,
        ToCached,
    },
    utils::get_keys,
};

use redis::{AsyncCommands, FromRedisValue, ToRedisArgs};
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, iter};
use tokio::time::{sleep, Duration};
use tracing::warn;
use twilight_gateway::Cluster;
use twilight_model::{
    application::interaction::Interaction,
    channel::{Channel, GuildChannel},
    gateway::event::Event,
    id::{GuildId, UserId},
};

pub async fn get<K, T>(conn: &mut redis::aio::Connection, key: K) -> ApiResult<Option<T>>
where
    K: ToRedisArgs + Send + Sync,
    T: DeserializeOwned,
{
    let res: Option<String> = conn.get(key).await?;

    Ok(res
        .map(|mut value| simd_json::from_str(value.as_mut_str()))
        .transpose()?)
}

pub async fn get_members<K, T>(conn: &mut redis::aio::Connection, key: K) -> ApiResult<Vec<T>>
where
    K: ToRedisArgs + Send + Sync,
    T: FromRedisValue,
{
    let res = conn.smembers(key).await?;

    Ok(res)
}

pub async fn get_members_len<K>(conn: &mut redis::aio::Connection, key: K) -> ApiResult<u64>
where
    K: ToRedisArgs + Send + Sync,
{
    let res = conn.scard(key).await?;

    Ok(res)
}

pub async fn set<K, T>(conn: &mut redis::aio::Connection, key: K, value: T) -> ApiResult<()>
where
    K: AsRef<str>,
    T: Serialize,
{
    set_all(conn, iter::once((key, value))).await?;

    Ok(())
}

pub async fn set_all<I, K, T>(conn: &mut redis::aio::Connection, keys: I) -> ApiResult<()>
where
    I: IntoIterator<Item = (K, T)>,
    K: AsRef<str>,
    T: Serialize,
{
    let mut members = HashMap::new();

    let keys = keys
        .into_iter()
        .map(|(key, value)| {
            let key = key.as_ref();
            let parts = get_keys(key);

            let new_key = if parts.len() > 2 && parts[0] == CHANNEL_KEY {
                format!("{}:{}", parts[0], parts[2])
            } else {
                key.to_owned()
            };

            if parts.len() > 1 {
                members
                    .entry(format!("{}{}", parts[0], KEYS_SUFFIX))
                    .or_insert_with(Vec::new)
                    .push(new_key.clone());
            }

            if parts.len() > 2 && parts[0] != MESSAGE_KEY {
                members
                    .entry(format!("{}{}:{}", GUILD_KEY, KEYS_SUFFIX, parts[1]))
                    .or_insert_with(Vec::new)
                    .push(new_key.clone());
            } else if parts.len() > 2 {
                members
                    .entry(format!("{}{}:{}", CHANNEL_KEY, KEYS_SUFFIX, parts[1]))
                    .or_insert_with(Vec::new)
                    .push(new_key.clone());
            }

            simd_json::to_string(&value)
                .map(|value| (new_key, value))
                .map_err(ApiError::from)
        })
        .collect::<ApiResult<Vec<(String, String)>>>()?;

    if keys.is_empty() {
        return Ok(());
    }

    conn.set_multiple(keys.as_slice()).await?;

    for (key, value) in members {
        conn.sadd(key, value.as_slice()).await?;
    }

    Ok(())
}

pub async fn del_all<I, K>(conn: &mut redis::aio::Connection, keys: I) -> ApiResult<()>
where
    I: IntoIterator<Item = K>,
    K: AsRef<str>,
{
    let mut members = HashMap::new();

    let keys = keys
        .into_iter()
        .map(|key| {
            let key = key.as_ref();
            let parts = get_keys(key);

            let new_key = if parts.len() > 2 && parts[0] == CHANNEL_KEY {
                format!("{}:{}", parts[0], parts[2])
            } else {
                key.to_owned()
            };

            if parts.len() > 1 {
                members
                    .entry(format!("{}{}", parts[0], KEYS_SUFFIX))
                    .or_insert_with(Vec::new)
                    .push(new_key.clone());
            }

            if parts.len() > 2 {
                if parts[0] != MESSAGE_KEY {
                    members
                        .entry(format!("{}{}:{}", GUILD_KEY, KEYS_SUFFIX, parts[1]))
                        .or_insert_with(Vec::new)
                        .push(new_key.clone());
                } else {
                    members
                        .entry(format!("{}{}:{}", CHANNEL_KEY, KEYS_SUFFIX, parts[1]))
                        .or_insert_with(Vec::new)
                        .push(new_key.clone());
                }
            }

            new_key
        })
        .collect::<Vec<String>>();

    if keys.is_empty() {
        return Ok(());
    }

    conn.del(keys).await?;

    for (key, value) in members {
        conn.srem(key, value).await?;
    }

    Ok(())
}

pub async fn del(conn: &mut redis::aio::Connection, key: impl AsRef<str>) -> ApiResult<()> {
    del_all(conn, iter::once(key)).await?;

    Ok(())
}

pub async fn run_jobs(conn: &mut redis::aio::Connection, clusters: &[Cluster]) {
    loop {
        let mut statuses = vec![];
        let mut sessions = HashMap::new();

        for cluster in clusters {
            let mut status: Vec<StatusInfo> = cluster
                .info()
                .into_iter()
                .map(|(k, v)| StatusInfo {
                    shard: k,
                    status: format!("{}", v.stage()),
                    latency: v
                        .latency()
                        .recent()
                        .back()
                        .map(|value| value.as_millis() as u64)
                        .unwrap_or_default(),
                    last_ack: v
                        .latency()
                        .received()
                        .map(|value| {
                            FormattedDateTime::now()
                                - time::Duration::milliseconds(value.elapsed().as_millis() as i64)
                        })
                        .unwrap_or_else(FormattedDateTime::now),
                })
                .collect();

            statuses.append(&mut status);

            for (shard, info) in cluster.info() {
                sessions.insert(
                    shard.to_string(),
                    SessionInfo {
                        session_id: info.session_id().unwrap_or_default().to_owned(),
                        sequence: info.seq(),
                    },
                );
            }
        }

        statuses.sort_by(|a, b| a.shard.cmp(&b.shard));

        if let Err(err) = set(conn, STATUSES_KEY, &statuses).await {
            warn!("Failed to dump gateway statuses: {:?}", err);
        }

        if let Err(err) = set(conn, SESSIONS_KEY, &sessions).await {
            warn!("Failed to dump gateway sessions: {:?}", err);
        }

        sleep(Duration::from_millis(CACHE_DUMP_INTERVAL as u64)).await;
    }
}

async fn clear_guild(conn: &mut redis::aio::Connection, guild_id: GuildId) -> ApiResult<()> {
    let members: Vec<String> =
        get_members(conn, format!("{}{}:{}", GUILD_KEY, KEYS_SUFFIX, guild_id)).await?;

    del_all(conn, members).await?;
    del(conn, guild_key(guild_id)).await?;

    Ok(())
}

pub async fn update(
    conn: &mut redis::aio::Connection,
    event: &Event,
    bot_id: UserId,
) -> ApiResult<()> {
    match event {
        Event::ChannelCreate(data) => {
            if let Channel::Guild(c) = &data.0 {
                set(conn, channel_key(c.guild_id().unwrap(), c.id()), c).await?;
            }
        }
        Event::ChannelDelete(data) => {
            if let Channel::Guild(c) = &data.0 {
                del(conn, channel_key(c.guild_id().unwrap(), c.id())).await?;
            }
        }
        Event::ChannelUpdate(data) => {
            if let Channel::Guild(c) = &data.0 {
                set(conn, channel_key(c.guild_id().unwrap(), c.id()), c).await?;
            }
        }
        Event::GuildCreate(data) => {
            clear_guild(conn, data.id).await?;

            let mut items = vec![];
            let mut guild = data.clone();
            for channel in guild.channels.drain(..) {
                if let GuildChannel::Text(mut channel) = channel {
                    channel.guild_id = Some(data.id);

                    items.push((
                        channel_key(data.id, channel.id),
                        GuildItem::Channel(GuildChannel::Text(channel)),
                    ));
                }
            }

            set_all(conn, items).await?;

            // Cache roles
            let roles = data
                .roles
                .iter()
                .map(|role| (role_key(data.id, role.id), role.to_cached()));

            set_all(conn, roles).await?;

            // Cache users
            let users = data
                .members
                .iter()
                .map(|member| member.user.to_cached())
                .map(|user| (user_key(user.id), user));

            set_all(conn, users).await?;

            // Cache members
            let members = data.members.iter().map(|member| {
                let key = member_key(data.id, member.user.id);

                (key, member.to_cached())
            });

            set_all(conn, members).await?;

            // Cache the guild itself
            set(conn, guild_key(data.id), data.to_cached()).await?;
        }
        Event::GuildDelete(data) => clear_guild(conn, data.id).await?,
        Event::GuildUpdate(data) => set(conn, guild_key(data.id), &data).await?,
        Event::InteractionCreate(data) => {
            let (guild, member, user) = match &data.0 {
                Interaction::ApplicationCommand(data) => (data.guild_id, &data.member, &data.user),
                Interaction::MessageComponent(data) => (data.guild_id, &data.member, &data.user),
                _ => return Ok(()),
            };

            if let Some(user) = user {
                let user_key = user_key(user.id);
                set(conn, &user_key, user.to_cached()).await?;
            }

            if let (Some(member), Some(guild_id)) = (member, guild) {
                if let Some(user) = &member.user {
                    let user_key = user_key(user.id);
                    set(conn, &user_key, user.to_cached()).await?;

                    let member_key = member_key(guild_id, user.id);
                    let member = member.to_cached().into_member(guild_id, user.id);
                    set(conn, &member_key, member).await?;
                }
            }
        }
        Event::MemberAdd(data) => {
            if CONFIG.state_member {
                let user_key = user_key(data.user.id);
                set(conn, &user_key, data.user.to_cached()).await?;

                let member_key = member_key(data.guild_id, data.user.id);
                set(conn, &member_key, data.to_cached()).await?;
            }
        }
        Event::MemberRemove(data) => {
            if CONFIG.state_member {
                let key = member_key(data.guild_id, data.user.id);
                del(conn, &key).await?;
            }
        }
        Event::MemberUpdate(data) => {
            if CONFIG.state_member || data.user.id == bot_id {
                let user_key = user_key(data.user.id);
                set(conn, &user_key, data.user.to_cached()).await?;

                let member_key = member_key(data.guild_id, data.user.id);

                if let Some(mut member) = get::<_, CachedMember>(conn, &member_key).await? {
                    member.nick = data.nick.clone();
                    member.roles = data.roles.clone();
                    member.user_id = data.user.id;
                    set(conn, &member_key, &member).await?;
                }
            }
        }
        Event::MemberChunk(data) => {
            if CONFIG.state_member {
                let keys = data
                    .members
                    .iter()
                    .map(|member| member.user.to_cached())
                    .map(|user| (user_key(user.id), user));

                set_all(conn, keys).await?;

                let keys = data
                    .members
                    .iter()
                    .map(|member| member.to_cached())
                    .map(|member| (member_key(data.guild_id, member.user_id), member));

                set_all(conn, keys).await?;
            }
        }
        Event::MessageCreate(data) => {
            let user_key = user_key(data.author.id);
            set(conn, &user_key, data.author.to_cached()).await?;
        }
        Event::ReactionAdd(data) => {
            if let Some(member) = &data.member {
                let user_key = user_key(member.user.id);
                set(conn, &user_key, member.user.to_cached()).await?;

                let member_key = member_key(member.guild_id, member.user.id);
                set(conn, &member_key, member.to_cached()).await?;
            }
        }
        Event::ReactionRemove(data) => {
            if let Some(member) = &data.member {
                let user_key = user_key(member.user.id);
                set(conn, &user_key, member.user.to_cached()).await?;

                let member_key = member_key(member.guild_id, member.user.id);
                set(conn, &member_key, member.to_cached()).await?;
            }
        }
        Event::Ready(data) => {
            set(conn, BOT_USER_KEY, data.user.to_cached()).await?;
            let keys = data.guilds.iter().map(|guild| (guild_key(guild.id), guild));
            set_all(conn, keys).await?;
        }
        Event::RoleCreate(data) => {
            let key = role_key(data.guild_id, data.role.id);
            set(conn, &key, data.role.to_cached()).await?;
        }
        Event::RoleDelete(data) => del(conn, role_key(data.guild_id, data.role_id)).await?,
        Event::RoleUpdate(data) => {
            let key = role_key(data.guild_id, data.role.id);
            set(conn, &key, data.role.to_cached()).await?;
        }
        Event::UnavailableGuild(data) => {
            clear_guild(conn, data.id).await?;
            set(conn, guild_key(data.id), data).await?;
        }
        Event::UserUpdate(data) => set(conn, BOT_USER_KEY, data.to_cached()).await?,

        Event::ThreadCreate(_) => todo!(),
        Event::ThreadDelete(_) => todo!(),
        Event::ThreadUpdate(_) => todo!(),
        _ => {}
    }

    Ok(())
}
