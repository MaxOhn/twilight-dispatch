use twilight_model::id::{ChannelId, GuildId, RoleId, UserId};

pub const EXCHANGE: &str = "gateway";
pub const QUEUE_RECV: &str = "gateway.recv";
pub const QUEUE_SEND: &str = "gateway.send";

pub const SESSIONS_KEY: &str = "gateway_sessions";
pub const STATUSES_KEY: &str = "gateway_statuses";
pub const STARTED_KEY: &str = "gateway_started";
pub const SHARDS_KEY: &str = "gateway_shards";

pub const BOT_USER_KEY: &str = "bot_user";
pub const GUILD_KEY: &str = "guild";
pub const CHANNEL_KEY: &str = "channel";
pub const MESSAGE_KEY: &str = "message";
pub const ROLE_KEY: &str = "role";
pub const MEMBER_KEY: &str = "member";

pub const KEYS_SUFFIX: &str = "_keys";

pub const CACHE_DUMP_INTERVAL: usize = 1000;
pub const METRICS_DUMP_INTERVAL: usize = 1000;

pub const CONNECT_COLOR: usize = 0x1F8B4C;
pub const DISCONNECT_COLOR: usize = 0xE74C3C;
pub const READY_COLOR: usize = 0x1F8B4C;
pub const RESUME_COLOR: usize = 0x1E90FF;

pub fn guild_key(guild: GuildId) -> String {
    format!("{}:{}", GUILD_KEY, guild)
}

pub fn channel_key(guild: GuildId, channel: ChannelId) -> String {
    format!("{}:{}:{}", CHANNEL_KEY, guild, channel)
}

pub fn private_channel_key(channel: ChannelId) -> String {
    format!("{}:{}", CHANNEL_KEY, channel)
}

pub fn role_key(guild: GuildId, role: RoleId) -> String {
    format!("{}:{}:{}", ROLE_KEY, guild, role)
}

pub fn member_key(guild: GuildId, member: UserId) -> String {
    format!("{}:{}:{}", MEMBER_KEY, guild, member)
}
