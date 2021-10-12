use hyper::{http::Error as HyperHTTPError, Error as HyperError};
use lapin::Error as LapinError;
use prometheus::Error as PrometheusError;
use redis::RedisError;
use serde::{de::Error as SerdeDeError, Deserialize, Deserializer, Serialize, Serializer};
use serde_repr::{Deserialize_repr, Serialize_repr};
use simd_json::{owned::Value, Error as SimdJsonError};
use std::{
    env::VarError,
    error::Error,
    fmt::{self, Display, Formatter},
    io::Error as IoError,
    net::AddrParseError,
    num::ParseIntError,
    ops::{Add, Sub},
};
use time::{Duration, OffsetDateTime};
use twilight_gateway::{cluster::ClusterStartError, shard::LargeThresholdError};
use twilight_model::{
    channel::GuildChannel,
    gateway::OpCode,
    guild::{Guild, Member, PartialMember, Permissions, Role},
    id::{GuildId, RoleId, UserId},
    user::{CurrentUser, User},
};

#[derive(Debug, Clone)]
pub struct FormattedDateTime(OffsetDateTime);

impl FormattedDateTime {
    pub fn now() -> Self {
        Self(OffsetDateTime::now_utc())
    }
}

impl Sub<Duration> for FormattedDateTime {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        Self(self.0.sub(rhs))
    }
}

impl Sub<FormattedDateTime> for FormattedDateTime {
    type Output = Duration;

    fn sub(self, rhs: FormattedDateTime) -> Self::Output {
        self.0.sub(rhs.0)
    }
}

impl Add<Duration> for FormattedDateTime {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0.add(rhs))
    }
}

impl Serialize for FormattedDateTime {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0.format("%FT%T.%N"))
    }
}

impl<'de> Deserialize<'de> for FormattedDateTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        match OffsetDateTime::parse(string + "+0000", "%FT%T.%N%z") {
            Ok(dt) => Ok(Self(dt)),
            Err(_) => Err(SerdeDeError::custom("not a valid formatted timestamp")),
        }
    }

    fn deserialize_in_place<D>(deserializer: D, place: &mut Self) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        match OffsetDateTime::parse(string + "+0000", "%FT%T.%N%z") {
            Ok(dt) => {
                place.0 = dt;
                Ok(())
            }
            Err(_) => Err(SerdeDeError::custom("not a valid formatted timestamp")),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub sequence: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StatusInfo {
    pub shard: u64,
    pub status: String,
    pub latency: u64,
    pub last_ack: FormattedDateTime,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PayloadInfo {
    pub op: OpCode,
    pub t: Option<String>,
    pub d: Value,
}

#[derive(Clone, Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum DeliveryOpcode {
    Send,
    Reconnect,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DeliveryInfo {
    pub op: DeliveryOpcode,
    pub shard: u64,
    pub data: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum GuildItem {
    Channel(GuildChannel),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CachedGuild {
    #[serde(rename = "a")]
    pub channels: Vec<GuildChannel>,
    #[serde(rename = "b")]
    pub icon: Option<String>,
    #[serde(rename = "c")]
    pub id: GuildId,
    #[serde(rename = "d", skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<Member>,
    #[serde(rename = "e")]
    pub name: String,
    #[serde(rename = "f")]
    pub owner_id: UserId,
    #[serde(rename = "g")]
    pub roles: Vec<Role>,
}

#[derive(Clone, Default, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct CachedCurrentUser {
    #[serde(rename = "a", skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,
    #[serde(rename = "b")]
    pub discriminator: String,
    #[serde(rename = "c")]
    pub id: UserId,
    #[serde(rename = "d")]
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct CachedUser {
    #[serde(rename = "a", skip_serializing_if = "Option::is_none")]
    pub avatar: Option<String>,
    #[serde(rename = "b")]
    pub bot: bool,
    #[serde(rename = "c")]
    pub discriminator: String,
    #[serde(rename = "d")]
    pub id: UserId,
    #[serde(rename = "e")]
    pub name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct CachedMember {
    #[serde(rename = "a")]
    pub guild_id: GuildId,
    #[serde(rename = "b")]
    pub nick: Option<String>,
    #[serde(rename = "c")]
    pub roles: Vec<RoleId>,
    #[serde(rename = "d")]
    pub user_id: UserId,
}

pub struct IntermediateMember {
    pub nick: Option<String>,
    pub roles: Vec<RoleId>,
}

impl IntermediateMember {
    pub fn into_member(self, guild_id: GuildId, user_id: UserId) -> CachedMember {
        CachedMember {
            guild_id,
            nick: self.nick,
            roles: self.roles,
            user_id,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct CachedRole {
    #[serde(rename = "a")]
    pub id: RoleId,
    #[serde(rename = "b")]
    pub name: String,
    #[serde(rename = "c")]
    pub permissions: Permissions,
    #[serde(rename = "d")]
    pub position: i64,
}

pub trait ToCached {
    type Kind;

    fn to_cached(&self) -> Self::Kind;
}

impl ToCached for Guild {
    type Kind = CachedGuild;

    fn to_cached(&self) -> Self::Kind {
        CachedGuild {
            channels: self.channels.to_owned(),
            icon: self.icon.to_owned(),
            id: self.id,
            members: self.members.to_owned(),
            name: self.name.to_owned(),
            owner_id: self.owner_id,
            roles: self.roles.to_owned(),
        }
    }
}

impl ToCached for CurrentUser {
    type Kind = CachedCurrentUser;

    fn to_cached(&self) -> Self::Kind {
        CachedCurrentUser {
            avatar: self.avatar.to_owned(),
            discriminator: self.discriminator.to_owned(),
            id: self.id,
            name: self.name.to_owned(),
        }
    }
}

impl ToCached for Role {
    type Kind = CachedRole;

    fn to_cached(&self) -> Self::Kind {
        CachedRole {
            id: self.id,
            name: self.name.to_owned(),
            permissions: self.permissions,
            position: self.position,
        }
    }
}

impl ToCached for Member {
    type Kind = CachedMember;

    fn to_cached(&self) -> Self::Kind {
        CachedMember {
            guild_id: self.guild_id,
            nick: self.nick.to_owned(),
            roles: self.roles.to_owned(),
            user_id: self.user.id,
        }
    }
}

impl ToCached for PartialMember {
    type Kind = IntermediateMember;

    fn to_cached(&self) -> Self::Kind {
        IntermediateMember {
            nick: self.nick.to_owned(),
            roles: self.roles.to_owned(),
        }
    }
}

impl ToCached for User {
    type Kind = CachedUser;

    fn to_cached(&self) -> Self::Kind {
        CachedUser {
            avatar: self.avatar.to_owned(),
            bot: self.bot,
            discriminator: self.discriminator.to_owned(),
            id: self.id,
            name: self.name.to_owned(),
        }
    }
}

pub type ApiResult<T> = Result<T, ApiError>;

#[derive(Debug)]
pub enum ApiError {
    Empty(()),
    SimdJson(SimdJsonError),
    Redis(RedisError),
    Var(VarError),
    ParseInt(ParseIntError),
    Lapin(LapinError),
    ClusterStart(ClusterStartError),
    LargeThreshold(LargeThresholdError),
    Hyper(HyperError),
    HyperHttp(HyperHTTPError),
    AddrParse(AddrParseError),
    Prometheus(PrometheusError),
    Io(IoError),
}

impl Error for ApiError {}

impl Display for ApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<()> for ApiError {
    fn from(_: ()) -> Self {
        Self::Empty(())
    }
}

impl From<SimdJsonError> for ApiError {
    fn from(err: SimdJsonError) -> Self {
        Self::SimdJson(err)
    }
}

impl From<RedisError> for ApiError {
    fn from(err: RedisError) -> Self {
        Self::Redis(err)
    }
}

impl From<VarError> for ApiError {
    fn from(err: VarError) -> Self {
        Self::Var(err)
    }
}

impl From<ParseIntError> for ApiError {
    fn from(err: ParseIntError) -> Self {
        Self::ParseInt(err)
    }
}

impl From<LapinError> for ApiError {
    fn from(err: LapinError) -> Self {
        Self::Lapin(err)
    }
}

impl From<ClusterStartError> for ApiError {
    fn from(err: ClusterStartError) -> Self {
        Self::ClusterStart(err)
    }
}

impl From<LargeThresholdError> for ApiError {
    fn from(err: LargeThresholdError) -> Self {
        Self::LargeThreshold(err)
    }
}

impl From<HyperError> for ApiError {
    fn from(err: HyperError) -> Self {
        Self::Hyper(err)
    }
}

impl From<HyperHTTPError> for ApiError {
    fn from(err: HyperHTTPError) -> Self {
        Self::HyperHttp(err)
    }
}

impl From<AddrParseError> for ApiError {
    fn from(err: AddrParseError) -> Self {
        Self::AddrParse(err)
    }
}

impl From<PrometheusError> for ApiError {
    fn from(err: PrometheusError) -> Self {
        Self::Prometheus(err)
    }
}

impl From<IoError> for ApiError {
    fn from(err: IoError) -> Self {
        Self::Io(err)
    }
}
