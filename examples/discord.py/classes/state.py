import asyncio
import copy
import datetime
import json
import logging
import inspect

from classes.guild import Guild
from discord.user import User, ClientUser
from discord.emoji import Emoji
from discord.partial_emoji import PartialEmoji
from discord.message import Message
from discord.channel import DMChannel, TextChannel, _channel_factory
from discord.raw_models import *
from discord.member import Member, VoiceState
from discord.role import Role
from discord.enums import ChannelType, try_enum
from discord import utils, Reaction
from discord.invite import Invite

log = logging.getLogger(__name__)


class State:
    def __init__(self, *, dispatch, handlers, hooks, loop, redis=None, shard_count=None, **options):
        self.dispatch = dispatch
        self.handlers = handlers
        self.hooks = hooks
        self.loop = loop
        self.redis = redis
        self.shard_count = shard_count

        self._ready_task = None
        self._ready_state = None
        self._ready_timeout = options.get("guild_ready_timeout", 2.0)

        self._voice_clients = {}
        self._private_channels_by_user = {}

        self.parsers = {}
        for attr, func in inspect.getmembers(self):
            if attr.startswith("parse_"):
                self.parsers[attr[6:].upper()] = func

    def _get(self, key):
        result = self.redis.get(key)
        if result:
            result = json.loads(result)
            result["_key"] = key
            if result.get("permission_overwrites"):
                result["permission_overwrites"] = [
                    {
                        "id": x["id"],
                        "type": "role" if x["type"] == 0 else "member",
                        "allow": int(x["allow"]),
                        "deny": int(x["deny"]),
                    }
                    for x in result["permission_overwrites"]
                ]
        return result

    def _scan(self, match):
        return list(self.redis.scan_iter(match=match))

    def _scan_get(self, match):
        matches = self._scan(match)
        if len(matches) >= 1:
            return self._get(matches[0])
        else:
            return None

    def _scan_and_get(self, match):
        matches = self._scan(match)
        return [self._get(match) for match in matches]

    def _keys(self, obj):
        keys = obj["_key"].split(":")
        return [int(x) for x in keys[1:]]

    @property
    def _users(self):
        results = self._scan_and_get(f"member:*:*")
        return list(set([User(state=self, data=x["user"]) for x in results]))

    @property
    def _emojis(self):
        results = self._scan_and_get("emoji:*:*")
        return [Emoji(guild=self._get_guild(self._keys(x)[0]), state=self, data=x) for x in results]

    @property
    def _guilds(self):
        results = self._scan_and_get("guild:*")
        return [Guild(state=self, data=x) for x in results]

    @property
    def _private_channels(self):
        results = self._scan_and_get("channel:*")
        return [DMChannel(me=self.user, state=self, data=x) for x in results if not x["guild_id"]]

    @property
    def _messages(self):
        results = self._scan_and_get("message:*:*")
        return [Message(channel=self.get_channel(int(x["channel_id"])), state=self, data=x) for x in results]

    def process_chunk_requests(self, guild_id, nonce, members, complete):
        return

    def call_handlers(self, key, *args, **kwargs):
        try:
            func = self.handlers[key]
        except KeyError:
            pass
        else:
            func(*args, **kwargs)

    async def call_hooks(self, key, *args, **kwargs):
        try:
            func = self.hooks[key]
        except KeyError:
            pass
        else:
            await func(*args, **kwargs)

    @property
    def user(self):
        return ClientUser(state=self, data=self._get("bot_user"))

    @property
    def self_id(self):
        return self.user.id

    @property
    def intents(self):
        return

    @property
    def voice_clients(self):
        return

    def _get_voice_client(self, guild_id):
        return

    def _add_voice_client(self, guild_id, voice):
        return

    def _remove_voice_client(self, guild_id):
        return

    def _update_references(self, ws):
        return

    def store_user(self, data):
        return User(state=self, data=data)

    def get_user(self, user_id):
        result = self._scan_get(f"member:*:{user_id}")
        if result:
            result = User(state=self, data=result["user"])
        return result

    def store_emoji(self, guild, data):
        return Emoji(guild=guild, state=self, data=data)

    @property
    def guilds(self):
        return self._guilds

    def _get_guild(self, guild_id):
        return Guild(state=self, data=self._get(f"guild:{guild_id}"))

    def _add_guild(self, guild):
        return

    def _remove_guild(self, guild):
        return

    @property
    def emojis(self):
        return self._emojis

    def get_emoji(self, emoji_id):
        result = self._scan_get(f"emoji:*:{emoji_id}")
        if result:
            result = Emoji(guild=self._get_guild(self._keys(result)[0]), state=self, data=result)
        return result

    @property
    def private_channels(self):
        return self._private_channels

    def _get_private_channel(self, channel_id):
        return DMChannel(me=self.user, state=self, data=self._get(f"channel:{channel_id}"))

    def _get_private_channel_by_user(self, user_id):
        return utils.find(lambda x: x.recipient.id == user_id, self.private_channels())

    def _add_private_channel(self, channel):
        return

    def add_dm_channel(self, data):
        return DMChannel(me=self.user, state=self, data=data)

    def _remove_private_channel(self, channel):
        return

    def _get_message(self, msg_id):
        result = self._scan_get(f"message:*:{msg_id}")
        if result:
            result = Message(channel=self.get_channel(self._keys(result)[0]), state=self, data=result)
        return result

    def _add_guild_from_data(self, guild):
        return Guild(state=self, data=guild)

    def _guild_needs_chunking(self, guild):
        return

    def _get_guild_channel(self, channel_id):
        result = self._scan_get(f"channel:*:{channel_id}")
        factory, _ = _channel_factory(result["type"])
        return factory(guild=self._get_guild(self._keys(result)[0]), state=self, data=result)

    async def chunker(self, guild_id, query="", limit=0, *, nonce=None):
        return

    async def query_members(self, guild, query, limit, user_ids, cache):
        return

    async def _delay_ready(self):
        try:
            while True:
                try:
                    guild = await asyncio.wait_for(self._ready_state.get(), timeout=self._ready_timeout)
                except asyncio.TimeoutError:
                    break
                else:
                    if guild.unavailable is False:
                        self.dispatch("guild_available", guild)
                    else:
                        self.dispatch("guild_join", guild)
            try:
                del self._ready_state
            except AttributeError:
                pass
        except asyncio.CancelledError:
            pass
        else:
            self.call_handlers("ready")
            self.dispatch("ready")
        finally:
            self._ready_task = None

    def parse_ready(self, data, old):
        if self._ready_task is not None:
            self._ready_task.cancel()
        self.dispatch("connect")
        self._ready_state = asyncio.Queue()
        self._ready_task = asyncio.ensure_future(self._delay_ready(), loop=self.loop)

    def parse_resumed(self, data, old):
        self.dispatch("resumed")

    def parse_message_create(self, data, old):
        message = self.create_message(channel=self.get_channel(int(data["channel_id"])), data=data)
        self.dispatch("message", message)

    def parse_message_delete(self, data, old):
        raw = RawMessageDeleteEvent(data)
        if old:
            old = self.create_message(channel=self.get_channel(int(data["channel_id"])), data=old)
            raw.cached_message = old
            self.dispatch("message_delete", old)
        self.dispatch("raw_message_delete", raw)

    def parse_message_delete_bulk(self, data, old):
        raw = RawBulkMessageDeleteEvent(data)
        if old:
            old = [self.create_message(channel=self.get_channel(int(x["channel_id"])), data=x) for x in old]
            raw.cached_messages = old
            self.dispatch("bulk_message_delete", old)
        self.dispatch("raw_bulk_message_delete", raw)

    def parse_message_update(self, data, old):
        raw = RawMessageUpdateEvent(data)
        if old:
            old = self.create_message(channel=self.get_channel(int(data["channel_id"])), data=old)
            raw.cached_message = old
            new = copy.copy(old)
            new._update(data)
            self.dispatch("message_edit", old, new)
        self.dispatch("raw_message_edit", raw)

    def parse_message_reaction_add(self, data, old):
        emoji = PartialEmoji.with_state(
            self,
            id=data["emoji"]["id"],
            animated=data["emoji"].get("animated", False),
            name=data["emoji"]["name"],
        )
        raw = RawReactionActionEvent(data, emoji, "REACTION_ADD")
        if data.get("member"):
            raw.member = Member(guild=self._get_guild(raw.guild_id), state=self, data=data.get("member"))
        self.dispatch("raw_reaction_add", raw)
        message = self._get_message(raw.message_id)
        if message:
            reaction = Reaction(message=message, data=data, emoji=self._upgrade_partial_emoji(emoji))
            user = raw.member or self._get_reaction_user(message.channel, raw.user_id)
            if user:
                self.dispatch("reaction_add", reaction, user)

    def parse_message_reaction_remove_all(self, data, old):
        raw = RawReactionClearEvent(data)
        self.dispatch("raw_reaction_clear", raw)
        message = self._get_message(raw.message_id)
        if message:
            self.dispatch("reaction_clear", message, None)

    def parse_message_reaction_remove(self, data, old):
        emoji = PartialEmoji.with_state(self, id=int(data["emoji"]["id"]), name=data["emoji"]["name"])
        raw = RawReactionActionEvent(data, emoji, "REACTION_REMOVE")
        self.dispatch("raw_reaction_remove", raw)
        message = self._get_message(raw.message_id)
        if message:
            reaction = Reaction(message=message, data=data, emoji=self._upgrade_partial_emoji(emoji))
            user = self._get_reaction_user(message.channel, raw.user_id)
            if user:
                self.dispatch("reaction_remove", reaction, user)

    def parse_message_reaction_remove_emoji(self, data, old):
        emoji = PartialEmoji.with_state(self, id=int(data["emoji"]["id"]), name=data["emoji"]["name"])
        raw = RawReactionClearEmojiEvent(data, emoji)
        self.dispatch("raw_reaction_clear_emoji", raw)
        message = self._get_message(raw.message_id)
        if message:
            reaction = Reaction(message=message, data=data, emoji=self._upgrade_partial_emoji(emoji))
            self.dispatch("reaction_clear_emoji", reaction)

    def parse_presence_update(self, data, old):
        guild = self._get_guild(int(data["guild_id"]))
        if not guild:
            return
        old_member = None
        member = guild.get_member(int(data["user"]["id"]))
        if member and old:
            old_member = Member._copy(member)
            user_update = old_member._presence_update(data=old, user=old["user"])
            if user_update:
                self.dispatch("user_update", user_update[1], user_update[0])
        self.dispatch("member_update", old_member, member)

    def parse_user_update(self, data, old):
        return

    def parse_invite_create(self, data, old):
        invite = Invite.from_gateway(state=self, data=data)
        self.dispatch("invite_create", invite)

    def parse_invite_delete(self, data, old):
        invite = Invite.from_gateway(state=self, data=data)
        self.dispatch("invite_delete", invite)

    def parse_channel_delete(self, data, old):
        if old and old["guild_id"]:
            factory, _ = _channel_factory(old["type"])
            channel = factory(guild=self._get_guild(int(data["guild_id"])), state=self, data=old)
            self.dispatch("guild_channel_delete", channel)
        elif old:
            channel = DMChannel(me=self.user, state=self, data=old)
            self.dispatch("private_channel_delete", channel)

    def parse_channel_update(self, data, old):
        channel_type = try_enum(ChannelType, data.get("type"))
        if old and channel_type is ChannelType.private:
            channel = DMChannel(me=self.user, state=self, data=data)
            old_channel = DMChannel(me=self.user, state=self, data=old)
            self.dispatch("private_channel_update", old_channel, channel)
        elif old:
            factory, _ = _channel_factory(data["type"])
            channel = factory(guild=self._get_guild(int(data["guild_id"])), state=self, data=data)
            old_factory, _ = _channel_factory(old["type"])
            old_channel = old_factory(guild=self._get_guild(int(old["guild_id"])), state=self, data=old)
            self.dispatch("guild_channel_update", old_channel, channel)

    def parse_channel_create(self, data, old):
        factory, ch_type = _channel_factory(data["type"])
        if ch_type is ChannelType.private:
            channel = DMChannel(me=self.user, data=data, state=self)
            self.dispatch("private_channel_create", channel)
        else:
            channel = factory(guild=self._get_guild(int(data["guild_id"])), state=self, data=data)
            self.dispatch("guild_channel_create", channel)

    def parse_channel_pins_update(self, data, old):
        channel = self.get_channel(int(data["channel_id"]))
        last_pin = utils.parse_time(data["last_pin_timestamp"]) if data["last_pin_timestamp"] else None
        try:
            channel.guild
        except AttributeError:
            self.dispatch("private_channel_pins_update", channel, last_pin)
        else:
            self.dispatch("guild_channel_pins_update", channel, last_pin)

    def parse_channel_recipient_add(self, data, old):
        return

    def parse_channel_recipient_remove(self, data, old):
        return

    def parse_guild_member_add(self, data, old):
        member = Member(guild=self._get_guild(int(data["guild_id"])), data=data, state=self)
        self.dispatch("member_join", member)

    def parse_guild_member_remove(self, data, old):
        if old:
            member = Member(guild=self._get_guild(int(data["guild_id"])), data=old, state=self)
            self.dispatch("member_remove", member)

    def parse_guild_member_update(self, data, old):
        guild = self._get_guild(int(data["guild_id"]))
        member = guild.get_member(int(data["user"]["id"]))
        if member:
            old_member = Member._copy(member)
            old_member._update(old)
            user_update = old_member._update_inner_user(data["user"])
            if user_update:
                self.dispatch("user_update", user_update[1], user_update[0])
            self.dispatch("member_update", old_member, member)

    def parse_guild_emojis_update(self, data, old):
        guild = self._get_guild(int(data["guild_id"]))
        if guild:
            before_emojis = None
            if old:
                before_emojis = [self.store_emoji(guild, x) for x in old]
            after_emojis = tuple(map(lambda x: self.store_emoji(guild, x), data["emojis"]))
            self.dispatch("guild_emojis_update", guild, before_emojis, after_emojis)

    def _get_create_guild(self, data):
        return self._add_guild_from_data(data)

    async def chunk_guild(self, guild, *, wait=True, cache=None):
        return

    async def _chunk_and_dispatch(self, guild, unavailable):
        return

    def parse_guild_create(self, data, old):
        unavailable = data.get("unavailable")
        if unavailable is True:
            return
        guild = self._get_create_guild(data)
        try:
            self._ready_state.put_nowait(guild)
        except AttributeError:
            if unavailable is False:
                self.dispatch("guild_available", guild)
            else:
                self.dispatch("guild_join", guild)

    def parse_guild_sync(self, data, old):
        return

    def parse_guild_update(self, data, old):
        guild = self._get_guild(int(data["id"]))
        if guild:
            old_guild = None
            if old:
                old_guild = copy.copy(guild)
                old_guild = old_guild._from_data(old)
            self.dispatch("guild_update", old_guild, guild)

    def parse_guild_delete(self, data, old):
        if old:
            old = Guild(state=self, data=old)
            if data.get("unavailable", False):
                new = Guild(state=self, data=data)
                self.dispatch("guild_unavailable", new)
            else:
                self.dispatch("guild_remove", old)

    def parse_guild_ban_add(self, data, old):
        guild = self._get_guild(int(data["guild_id"]))
        if guild:
            user = self.store_user(data["user"])
            member = guild.get_member(user.id) or user
            self.dispatch("member_ban", guild, member)

    def parse_guild_ban_remove(self, data, old):
        guild = self._get_guild(int(data["guild_id"]))
        if guild:
            self.dispatch("member_unban", guild, self.store_user(data["user"]))

    def parse_guild_role_create(self, data, old):
        role = Role(guild=self._get_guild(int(data["guild_id"])), state=self, data=data["role"])
        self.dispatch("guild_role_create", role)

    def parse_guild_role_delete(self, data, old):
        if old:
            role = Role(guild=self._get_guild(int(data["guild_id"])), state=self, data=old)
            self.dispatch("guild_role_delete", role)

    def parse_guild_role_update(self, data, old):
        if old:
            role = Role(guild=self._get_guild(int(data["guild_id"])), state=self, data=data["role"])
            old_role = Role(guild=self._get_guild(int(data["guild_id"])), state=self, data=old)
            self.dispatch("guild_role_update", old_role, role)

    def parse_guild_members_chunk(self, data, old):
        return

    def parse_guild_integrations_update(self, data, old):
        self.dispatch("guild_integrations_update", self._get_guild(int(data["guild_id"])))

    def parse_webhooks_update(self, data, old):
        self.dispatch("webhooks_update", self.get_channel(int(data["channel_id"])))

    def parse_voice_state_update(self, data, old):
        guild = self._get_guild(int(data["guild_id"]))
        if guild:
            member = guild.get_member(data["user_id"])
            before = None
            after = VoiceState(data=data, channel=self.get_channel(data["channel_id"]))
            if old:
                before = VoiceState(data=data, channel=self.get_channel(old["channel_id"]))
            self.dispatch("voice_state_update", member, before, after)

    def parse_voice_server_update(self, data, old):
        return

    def parse_typing_start(self, data, old):
        channel = self._get_guild_channel(data["channel_id"])
        if channel:
            member = None
            if isinstance(channel, DMChannel):
                member = channel.recipient
            elif isinstance(channel, TextChannel):
                guild = self._get_guild(data["guild_id"])
                if guild:
                    member = guild.get_member(data["user_id"])
            if member:
                self.dispatch("typing", channel, member, datetime.datetime.utcfromtimestamp(data.get("timestamp")))

    def parse_relationship_add(self, data, old):
        return

    def parse_relationship_remove(self, data, old):
        return

    def _get_reaction_user(self, channel, user_id):
        if isinstance(channel, TextChannel):
            return channel.guild.get_member(user_id)
        return self.get_user(user_id)

    def get_reaction_emoji(self, data):
        return self.get_emoji(int(data["id"]))

    def _upgrade_partial_emoji(self, emoji):
        return self.get_emoji(emoji.id)

    def get_channel(self, channel_id):
        if not channel_id:
            return None
        return self._get_private_channel(channel_id) or self._get_guild_channel(channel_id)

    def create_message(self, *, channel, data):
        return Message(state=self, channel=channel, data=data)
