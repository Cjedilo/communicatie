"""
Client-facing WebSocket handler  (/ws)

All messages are JSON:  {"type": "...", ...params}
All responses are JSON: {"type": "...", "ok": true/false, ...data}

Authentication is via the httpOnly session cookie already set when the
page loaded.  No credentials are sent over the WebSocket.
"""

import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import aiohttp
from aiohttp import web

import auth
import config
import db
import db_config
from federation import get_remote_messages, notify_peers_of_message
from utils import _dumps, _ok, _err, _str_uuid

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Simple in-memory rate limiter (token bucket per user)
# ---------------------------------------------------------------------------

_rate_buckets: dict[uuid.UUID, tuple[float, int]] = {}  # user_id → (last_refill, tokens)
_WINDOW = 60.0


def _rate_ok(user_id: uuid.UUID, limit: int) -> bool:
    now = time.monotonic()
    last, tokens = _rate_buckets.get(user_id, (now, limit))
    elapsed = now - last
    tokens = min(limit, tokens + int(elapsed * limit / _WINDOW))
    if tokens <= 0:
        _rate_buckets[user_id] = (now, tokens)
        return False
    _rate_buckets[user_id] = (now, tokens - 1)
    return True


# ---------------------------------------------------------------------------
# Active WebSocket connections per channel (for live push)
# ---------------------------------------------------------------------------

_channel_sockets:      dict[uuid.UUID, set[web.WebSocketResponse]] = defaultdict(set)
_stream_sockets:       set[web.WebSocketResponse] = set()
_notification_sockets: set[web.WebSocketResponse] = set()  # every connected client WS


def _subscribe_to_channel(channel_id: uuid.UUID, ws: web.WebSocketResponse):
    _channel_sockets[channel_id].add(ws)


def _unsubscribe_from_channel(channel_id: uuid.UUID, ws: web.WebSocketResponse):
    _channel_sockets[channel_id].discard(ws)


def _unsubscribe_all(ws: web.WebSocketResponse):
    for sockets in _channel_sockets.values():
        sockets.discard(ws)
    _stream_sockets.discard(ws)
    # _notification_sockets is NOT cleared here — it persists for the full
    # connection lifetime and is only removed when the socket actually closes.


async def push_notification(channel_id: uuid.UUID, peer_id: uuid.UUID | None = None,
                             mentioned_user_ids: list | None = None):
    """Lightweight push to every connected client so they can update unread badges."""
    msg  = _dumps({"type": "new_message_notification", "ok": True,
                   "channel_id": channel_id, "peer_id": peer_id,
                   "mentions": [str(u) for u in (mentioned_user_ids or [])]})
    dead = set()
    for ws in list(_notification_sockets):
        try:
            await ws.send_str(msg)
        except Exception:
            dead.add(ws)
    for ws in dead:
        _notification_sockets.discard(ws)


async def broadcast_to_channel(channel_id: uuid.UUID, payload: dict):
    """Push a message to all connected clients watching this channel."""
    msg  = _dumps(payload)
    dead = set()
    for ws in list(_channel_sockets.get(channel_id, [])):
        try:
            await ws.send_str(msg)
        except Exception:
            dead.add(ws)
    for ws in dead:
        _channel_sockets[channel_id].discard(ws)


async def push_to_stream(payload: dict):
    """Push a stream_message or stream_update event to all stream viewers."""
    if not _stream_sockets:
        return
    msg  = _dumps(payload)
    dead = set()
    for ws in list(_stream_sockets):
        try:
            await ws.send_str(msg)
        except Exception:
            dead.add(ws)
    for ws in dead:
        _stream_sockets.discard(ws)



def _require_str(data: dict, key: str, max_len: int = 256) -> str | None:
    val = data.get(key, "")
    if not isinstance(val, str) or not val.strip():
        return None
    return val.strip()[:max_len]


async def _check_channel_access(channel_id: uuid.UUID, user_id: uuid.UUID) -> tuple[dict | None, str | None]:
    """Returns (channel, error_reason)."""
    channel = await db.channel_by_id(channel_id)
    if not channel:
        return None, "Channel not found"
    if not channel["public"]:
        if not await db.is_member(channel_id, user_id):
            return None, "Not a member of this private channel"
    return channel, None


async def _can_manage_channel(channel: dict, user_id: uuid.UUID) -> bool:
    """A channel's creator and its appointed moderators can manage it — the
    server owner has no special rights over chats someone else made."""
    if channel["created_by"] == user_id:
        return True
    return await db.is_channel_moderator(channel["id"], user_id)


# ---------------------------------------------------------------------------
# Message handlers
# ---------------------------------------------------------------------------

async def _handle_login(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    # Login is handled via HTTP POST — the WS is only opened after login.
    # This handler exists so the client can confirm the session is still valid.
    return _ok("login", user_id=session["user_id"], name=session["name"], avatar=session.get("avatar"))


async def _handle_logout(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    token = session.get("_token")
    if token:
        await auth.delete_session(token)
    return _ok("logout")


async def _handle_read_stream(data: dict, session: dict, ws: web.WebSocketResponse) -> None:
    """Stream: local messages immediately; remote pushed per-peer with per-channel cursors."""
    is_initial = not data.get("paginate", False)
    if is_initial:
        _stream_sockets.add(ws)

    # Local cursor: oldest local message timestamp the client already has
    local_before = None
    local_before_str = data.get("local_before")
    if local_before_str:
        try:
            local_before = datetime.fromisoformat(local_before_str)
        except (ValueError, TypeError):
            pass

    # Per-channel remote cursors: { "channel_id|peer_id": iso_ts }
    remote_cursors = data.get("remote_cursors") or {}

    peer_list = await db.peers_all()
    settings  = await db.user_settings_get(session["user_id"])
    subscribed = set(settings.get("stream_subscribed") or [])

    messages  = await db.stream_messages(session["user_id"], before=local_before)
    has_more  = (len(messages) == 60) or (is_initial and len(peer_list) > 0)
    await ws.send_str(_dumps(_ok("read_stream", messages=messages, has_more=has_more)))

    from federation import get_peer_stream_messages

    async def _push_peer(peer):
        try:
            msgs = await get_peer_stream_messages(peer, channel_cursors=remote_cursors,
                                                   subscribed_channels=subscribed)
            if not ws.closed:
                # has_more=True when peer returned results (may have more older pages)
                await ws.send_str(_dumps({
                    "type":     "stream_update",
                    "ok":       True,
                    "messages": msgs,
                    "has_more": len(msgs) > 0,
                }))
        except Exception as e:
            log.warning("stream peer push failed for %s: %s", peer.get("address"), e)

    for peer in peer_list:
        asyncio.create_task(_push_peer(peer))


async def _handle_toggle_stream_channel(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("toggle_stream_channel", "Missing channel_id")
    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("toggle_stream_channel", "Channel not found")
    if not await _can_manage_channel(channel, session["user_id"]):
        return _err("toggle_stream_channel", "Not authorised")
    new_val = not channel.get("stream_excluded", False)
    await db.channel_set_stream_excluded(cid, new_val)
    return _ok("toggle_stream_channel", channel_id=cid, stream_excluded=new_val)


async def _forward_channel_setting(data: dict, session: dict, msg_type: str) -> dict | None:
    """If data contains peer_id, forward the setting change to that peer.
    Returns the handler result, or None to handle locally."""
    peer_id = _str_uuid(data.get("peer_id"))
    if not peer_id:
        return None
    peer = await db.peer_by_id(peer_id)
    if not peer:
        return _err(msg_type, "Unknown peer")
    from federation import set_channel_setting_on_peer
    kwargs = {k: v for k, v in data.items()
              if k not in ("type", "peer_id", "channel_id", "request_id")}
    ok, info = await set_channel_setting_on_peer(
        peer, data.get("channel_id"), session["user_id"], msg_type, **kwargs)
    if not ok:
        return _err(msg_type, info.get("reason") or "Could not update channel on remote server")
    return _ok(msg_type)


async def _handle_set_channel_allow_replies(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    fwd = await _forward_channel_setting(data, session, "set_channel_allow_replies")
    if fwd is not None: return fwd
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("set_channel_allow_replies", "Missing channel_id")
    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("set_channel_allow_replies", "Channel not found")
    if not await _can_manage_channel(channel, session["user_id"]):
        return _err("set_channel_allow_replies", "Not authorised")
    allow_replies = bool(data.get("allow_replies", True))
    await db.channel_update_settings(cid, {"allow_replies": allow_replies})
    return _ok("set_channel_allow_replies", channel_id=cid, allow_replies=allow_replies)


async def _handle_set_channel_public(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    fwd = await _forward_channel_setting(data, session, "set_channel_public")
    if fwd is not None: return fwd
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("set_channel_public", "Missing channel_id")
    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("set_channel_public", "Channel not found")
    if not await _can_manage_channel(channel, session["user_id"]):
        return _err("set_channel_public", "Not authorised")
    public = bool(data.get("public", True))
    await db.channel_set_public(cid, public)
    if not public and channel.get("created_by"):
        # Switching to private must not lock the creator out of their own channel.
        await db.member_add(cid, channel["created_by"])
    return _ok("set_channel_public", channel_id=cid, public=public)


async def _handle_set_channel_post_restricted(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    fwd = await _forward_channel_setting(data, session, "set_channel_post_restricted")
    if fwd is not None: return fwd
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("set_channel_post_restricted", "Missing channel_id")
    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("set_channel_post_restricted", "Channel not found")
    if not await _can_manage_channel(channel, session["user_id"]):
        return _err("set_channel_post_restricted", "Not authorised")
    restricted = bool(data.get("post_restricted", False))
    await db.channel_update_settings(cid, {"post_restricted": restricted})
    return _ok("set_channel_post_restricted", channel_id=cid, post_restricted=restricted)


async def _handle_set_channel_restrict_replies(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    fwd = await _forward_channel_setting(data, session, "set_channel_restrict_replies")
    if fwd is not None: return fwd
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("set_channel_restrict_replies", "Missing channel_id")
    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("set_channel_restrict_replies", "Channel not found")
    if not await _can_manage_channel(channel, session["user_id"]):
        return _err("set_channel_restrict_replies", "Not authorised")
    restrict_replies = bool(data.get("restrict_replies", True))
    await db.channel_update_settings(cid, {"restrict_replies": restrict_replies})
    return _ok("set_channel_restrict_replies", channel_id=cid, restrict_replies=restrict_replies)


async def _handle_set_channel_allow_images(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    fwd = await _forward_channel_setting(data, session, "set_channel_allow_images")
    if fwd is not None: return fwd
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("set_channel_allow_images", "Missing channel_id")
    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("set_channel_allow_images", "Channel not found")
    if not await _can_manage_channel(channel, session["user_id"]):
        return _err("set_channel_allow_images", "Not authorised")
    allow_images = bool(data.get("allow_images", True))
    await db.channel_update_settings(cid, {"allow_images": allow_images})
    return _ok("set_channel_allow_images", channel_id=cid, allow_images=allow_images)


async def _handle_set_channel_allow_markdown(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    fwd = await _forward_channel_setting(data, session, "set_channel_allow_markdown")
    if fwd is not None: return fwd
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("set_channel_allow_markdown", "Missing channel_id")
    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("set_channel_allow_markdown", "Channel not found")
    if not await _can_manage_channel(channel, session["user_id"]):
        return _err("set_channel_allow_markdown", "Not authorised")
    allow_markdown = bool(data.get("allow_markdown", True))
    await db.channel_update_settings(cid, {"allow_markdown": allow_markdown})
    return _ok("set_channel_allow_markdown", channel_id=cid, allow_markdown=allow_markdown)


async def _handle_set_channel_allow_reactions(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    fwd = await _forward_channel_setting(data, session, "set_channel_allow_reactions")
    if fwd is not None: return fwd
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("set_channel_allow_reactions", "Missing channel_id")
    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("set_channel_allow_reactions", "Channel not found")
    if not await _can_manage_channel(channel, session["user_id"]):
        return _err("set_channel_allow_reactions", "Not authorised")
    allow_reactions = bool(data.get("allow_reactions", True))
    await db.channel_update_settings(cid, {"allow_reactions": allow_reactions})
    return _ok("set_channel_allow_reactions", channel_id=cid, allow_reactions=allow_reactions)


async def _handle_set_channel_edit_mode(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    fwd = await _forward_channel_setting(data, session, "set_channel_edit_mode")
    if fwd is not None: return fwd
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("set_channel_edit_mode", "Missing channel_id")
    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("set_channel_edit_mode", "Channel not found")
    if not await _can_manage_channel(channel, session["user_id"]):
        return _err("set_channel_edit_mode", "Not authorised")
    mode = data.get("edit_mode")
    if mode not in ("off", "history", "overwrite"):
        return _err("set_channel_edit_mode", "Invalid edit_mode")
    await db.channel_update_settings(cid, {"edit_mode": mode})
    return _ok("set_channel_edit_mode", channel_id=cid, edit_mode=mode)


async def _handle_set_channel_description(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    fwd = await _forward_channel_setting(data, session, "set_channel_description")
    if fwd is not None: return fwd
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("set_channel_description", "Missing channel_id")
    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("set_channel_description", "Channel not found")
    if not await _can_manage_channel(channel, session["user_id"]):
        return _err("set_channel_description", "Not authorised")
    description = str(data.get("description") or "").strip()[:512]
    await db.channel_update_settings(cid, {"description": description})
    return _ok("set_channel_description", channel_id=cid, description=description)


async def _handle_mark_channel_read(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("mark_channel_read", "Missing channel_id")
    await db.channel_mark_read(session["user_id"], cid)
    return _ok("mark_channel_read")


async def _handle_read_channels(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    channels  = await db.channels_visible_to(session["user_id"])
    peer_list = await db.peers_all()

    unread   = await db.channels_unread_counts(session["user_id"])
    mentions = await db.mentions_unread_counts(session["user_id"])
    for ch in channels:
        ch["unread_count"]  = unread.get(str(ch["id"]), 0)
        ch["mention_count"] = mentions.get(str(ch["id"]), 0)

    from federation import get_peer_channels
    remote = await asyncio.gather(*[get_peer_channels(p, session["user_id"]) for p in peer_list])
    for peer, chs in zip(peer_list, remote):
        peer["channels"] = chs

    host_address = await db.setting_get("peer_address") or _detect_address()
    return _ok("read_channels", channels=channels, peers=peer_list, host_address=host_address)


async def _handle_read_users(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    users = await db.users_all()
    from federation import get_peer_users
    peers        = await db.peers_all()
    remote_lists = await asyncio.gather(*[get_peer_users(p) for p in peers],
                                        return_exceptions=True)
    remote = []
    for r in remote_lists:
        if isinstance(r, list):
            remote.extend(r)
    return _ok("read_users", users=users, remote_users=remote)


async def _handle_read_user_messages(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    uid = _str_uuid(data.get("user_id"))
    pid = _str_uuid(data.get("peer_id"))
    if not uid:
        return _err("read_user_messages", "Missing user_id")

    if pid:
        peer = await db.peer_by_id(pid)
        if not peer:
            return _err("read_user_messages", "Unknown peer")
        from federation import get_peer_user_messages
        messages = await get_peer_user_messages(peer, uid)
    else:
        messages = await db.messages_by_user(uid, session["user_id"])

    return _ok("read_user_messages", messages=messages)


async def _handle_read_user(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    uid = _str_uuid(data.get("id"))
    if not uid:
        return _err("read_user", "Missing id")
    user = await db.user_by_id(uid)
    if not user:
        return _err("read_user", "User not found")
    user.pop("password", None)
    return _ok("read_user", user=user)


async def _handle_set_channel_icon(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    fwd = await _forward_channel_setting(data, session, "set_channel_icon")
    if fwd is not None: return fwd
    cid  = _str_uuid(data.get("channel_id"))
    icon = _require_str(data, "icon", 8)
    if not cid:
        return _err("set_channel_icon", "Missing channel_id")
    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("set_channel_icon", "Channel not found")
    if not await _can_manage_channel(channel, session["user_id"]):
        return _err("set_channel_icon", "Not authorised")
    await db.channel_set_icon(cid, icon)
    return _ok("set_channel_icon", icon=icon)


async def _handle_set_display_name(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    name = _require_str(data, "display_name", 128)
    await db.user_set_display_name(session["user_id"], name)
    user = await db.user_by_id(session["user_id"])
    return _ok("set_display_name", name=name or (user["name"] if user else ""))


async def _handle_read_profiles(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    raw_ids = data.get("ids", [])
    if not isinstance(raw_ids, list):
        return _err("read_profiles", "ids must be a list")
    ids = [_str_uuid(i) for i in raw_ids if _str_uuid(i)]
    users = await db.users_by_ids(ids)
    for u in users:
        u.pop("password", None)
    return _ok("read_profiles", users=users)



async def _handle_delete_user(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    uid = _str_uuid(data.get("id"))
    if not uid:
        return _err("delete_user", "Missing id")

    owner_id = await db.setting_get("owner_id")
    is_owner = str(session["user_id"]) == owner_id
    is_self  = session["user_id"] == uid

    if not is_owner and not is_self:
        return _err("delete_user", "Not authorised")

    await db.user_delete(uid)
    return _ok("delete_user")


async def _handle_read_user_settings(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    settings = await db.user_settings_get(session["user_id"])
    return _ok("read_user_settings", settings=settings)


async def _handle_set_user_setting(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    key = _require_str(data, "key", 64)
    if not key:
        return _err("set_user_setting", "Missing key")
    await db.user_setting_set(session["user_id"], key, data.get("value"))
    return _ok("set_user_setting", key=key)


async def _handle_start_chat(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    other_id  = _str_uuid(data.get("user_id")) or session["user_id"]
    other_pid = _str_uuid(data.get("peer_id"))   # None for local users
    is_self   = str(other_id) == str(session["user_id"]) and not other_pid

    existing = await db.channel_direct_find(session["user_id"], other_id, other_pid)
    if existing:
        return _ok("start_chat", channel_id=existing["id"])

    if is_self:
        other_name = "Scratchpad"
    elif other_pid:
        other_name = _require_str(data, "name", 128) or None
        if not other_name:
            cached = await db.user_cache_get(other_id, other_pid)
            other_name = cached.get("name") if cached else None
        other_name = other_name or "?"
    else:
        other = await db.user_by_id(other_id)
        other_name = (other.get("display_name") or other["name"]) if other else "?"

    if is_self:
        name = "Scratchpad"
    else:
        me    = await db.user_by_id(session["user_id"])
        names = sorted([(me.get("display_name") or me["name"]) if me else "?", other_name])
        name  = " & ".join(names)

    channel = await db.channel_create(name, False, session["user_id"])
    await db.member_add(channel["id"], session["user_id"])
    if not is_self:
        await db.member_add(channel["id"], other_id, other_pid)
        if other_pid and other_name:
            await db.user_cache_upsert(other_id, other_pid, other_name, None)
    return _ok("start_chat", channel_id=channel["id"])


async def _handle_create_channel(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    name            = _require_str(data, "name", 128)
    public          = bool(data.get("public", True))
    icon            = _require_str(data, "icon", 8)
    allow_replies   = bool(data.get("allow_replies", True))
    post_restricted = bool(data.get("post_restricted", False))
    edit_mode       = data.get("edit_mode")
    if edit_mode not in ("off", "history", "overwrite"):
        edit_mode = "off"
    description      = str(data.get("description") or "").strip()[:512]
    allow_images     = bool(data.get("allow_images", True))
    restrict_replies = bool(data.get("restrict_replies", True))
    allow_reactions  = bool(data.get("allow_reactions", True))
    allow_markdown   = bool(data.get("allow_markdown", True))
    if not name:
        return _err("create_channel", "Channel name required")

    channel = await db.channel_create(name, public, session["user_id"], icon,
                                       allow_replies, post_restricted, edit_mode, description,
                                       allow_images, restrict_replies, allow_reactions, allow_markdown)
    if not public:
        # Creator is automatically a member of private channels
        await db.member_add(channel["id"], session["user_id"])
    return _ok("create_channel", channel=channel)


async def _handle_delete_channel(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    cid = _str_uuid(data.get("id"))
    if not cid:
        return _err("delete_channel", "Missing id")

    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("delete_channel", "Channel not found")

    # Deletion is destructive enough that it stays creator-only — even
    # moderators don't get it, and the server owner has no override.
    if channel["created_by"] != session["user_id"]:
        return _err("delete_channel", "Not authorised")

    await db.channel_delete(cid)
    return _ok("delete_channel", id=cid)


async def _handle_read_channel(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    cid      = _str_uuid(data.get("id"))
    peer_id  = _str_uuid(data.get("peer_id"))

    if peer_id:
        # Remote channel — fetch index from peer, content stays on their server
        peer = await db.peer_by_id(peer_id)
        if not peer:
            return _err("read_channel", "Unknown peer")
        channel, messages = await get_remote_messages(peer, cid, session["user_id"])
        if channel and channel.get("access_denied"):
            return _err("read_channel", "Not authorised", channel_name=channel.get("name"),
                        owner=channel.get("owner"), peer_id=peer_id)
        if not channel:
            return _err("read_channel", "Could not reach that server, or the chat no longer exists")
        _subscribe_to_channel(cid, ws)   # receive live pushes from the peer
        return _ok("read_channel", channel=channel, messages=messages, remote=True,
                   host_address=peer["address"])

    if not cid:
        return _err("read_channel", "Missing id")

    channel, err = await _check_channel_access(cid, session["user_id"])
    if err:
        ch = await db.channel_by_id(cid)
        owner = await db.user_by_id(ch["created_by"]) if ch and ch.get("created_by") else None
        owner_info = {
            "id":     owner["id"],
            "name":   owner.get("display_name") or owner["name"],
            "avatar": owner.get("avatar"),
        } if owner else None
        return _err("read_channel", err, channel_name=(ch["name"] if ch else None), owner=owner_info)

    if channel.get("created_by"):
        creator = await db.user_by_id(channel["created_by"])
        if creator:
            channel["created_by_name"] = creator.get("display_name") or creator["name"]

    channel["can_manage"]    = await _can_manage_channel(channel, session["user_id"])
    channel["poster_count"]  = await db.channel_poster_count(cid)

    _subscribe_to_channel(cid, ws)
    messages = await db.messages_for_channel(cid)
    host_address = await db.setting_get("peer_address") or _detect_address()
    return _ok("read_channel", channel=channel, messages=messages, host_address=host_address)


async def _handle_message(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    if not _rate_ok(session["user_id"], config.RATE_LIMIT_MESSAGES):
        return _err("message", "Rate limit exceeded")

    cid      = _str_uuid(data.get("channel_id"))
    peer_id  = _str_uuid(data.get("peer_id"))
    parent   = _str_uuid(data.get("parent_id"))
    text     = _require_str(data, "text", 4096)
    image    = _require_str(data, "image", 512)

    if not cid:
        return _err("message", "Missing channel_id")
    if not text and not image:
        return _err("message", "Message must have text or image")

    msg_id  = uuid.uuid4()
    created = datetime.now(timezone.utc)
    user    = await db.user_by_id(session["user_id"])

    if peer_id:
        # ── Remote channel: content stays here, index goes to the channel server ──
        peer = await db.peer_by_id(peer_id)
        if not peer:
            return _err("message", "Unknown peer")

        our_address = await db.setting_get("peer_address") or _detect_address()
        await db.message_content_add(msg_id, text, image)

        base_url = our_address.replace("wss://", "https://").replace("ws://", "http://")
        avatar_url = f"{base_url}/img/{user['avatar']}" if user.get("avatar") else None

        from federation import send_message_to_peer
        ok, info = await send_message_to_peer(
            peer, cid, msg_id, session["user_id"], our_address, created,
            sender_name=user["name"] or "",
            sender_avatar=avatar_url,
            parent_id=parent,
            text=text,
            has_image=bool(image),
        )
        if not ok:
            channel_info = info.get("channel") or {}
            return _err("message", info.get("reason") or "Could not deliver message to remote server",
                        channel_name=channel_info.get("name"), owner=info.get("owner"), peer_id=peer_id)

    else:
        # ── Local channel: store index + content here ──
        channel, err = await _check_channel_access(cid, session["user_id"])
        if err:
            return _err("message", err)

        if channel["public"] and await db.is_banned(cid, session["user_id"]):
            return _err("message", "You are banned from this channel")

        if parent and not channel.get("allow_replies", True):
            return _err("message", "Replies are turned off in this channel")

        # post_restricted always gates top-level posts; replies are only
        # gated too when the channel's restrict_replies sub-option is on.
        restricted = channel.get("post_restricted") and (not parent or channel.get("restrict_replies", True))
        if restricted and not await _can_manage_channel(channel, session["user_id"]):
            return _err("message", "Only moderators can post in this channel")

        if image and not channel.get("allow_images", True):
            return _err("message", "Images are turned off in this channel")

        await db.message_index_add(msg_id, cid, session["user_id"], None, parent, created)
        await db.message_content_add(msg_id, text, image)

        # Detect and store @mentions for local users.
        import re as _re
        mentioned_names = list({m.lower() for m in _re.findall(r'@([\w]+)', text or "")})
        mentioned_ids: list[uuid.UUID] = []
        if mentioned_names:
            matched = await db.users_by_names(mentioned_names)
            mentioned_ids = [u["id"] for u in matched if u["id"] != session["user_id"]]
            if mentioned_ids:
                await db.message_mentions_store(msg_id, mentioned_ids)

        payload = _ok("message", message={
            "id":             msg_id,
            "channel_id":     cid,
            "sender_user_id": session["user_id"],
            "sender_peer_id": None,
            "sender_name":    user["name"],
            "sender_avatar":  user["avatar"],
            "parent_id":      parent,
            "text":           text,
            "image":          image,
            "created":        created,
        })

        await broadcast_to_channel(cid, payload)
        if not channel.get("stream_excluded"):
            await push_to_stream({"type": "stream_message", "ok": True, "message": {
                **payload["message"],
                "channel_name":     channel["name"],
                "channel_public":   channel["public"],
                "stream_excluded":  False,
            }})
        asyncio.create_task(notify_peers_of_message(
            cid, msg_id, session["user_id"], created,
            sender_name=user["name"] or "",
            sender_avatar=user["avatar"],
            parent_id=parent,
        ))
        await push_notification(cid, mentioned_user_ids=mentioned_ids)
        return payload

    return _ok("message", message={
        "id":             msg_id,
        "channel_id":     cid,
        "sender_user_id": session["user_id"],
        "sender_peer_id": None,
        "sender_name":    user["name"],
        "sender_avatar":  user["avatar"],
        "parent_id":      parent,
        "text":           text,
        "image":          image,
        "created":        created,
    })


async def _handle_edit_message(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    mid     = _str_uuid(data.get("id"))
    peer_id = _str_uuid(data.get("peer_id"))
    text    = _require_str(data, "text", 4096)
    image   = _require_str(data, "image", 512)

    if not mid:
        return _err("edit_message", "Missing id")
    if not text and not image:
        return _err("edit_message", "Message must have text or image")

    if peer_id:
        # The channel is hosted by a peer; we own this message's content
        # (we're the original sender), the peer owns the index/edit_mode.
        peer = await db.peer_by_id(peer_id)
        if not peer:
            return _err("edit_message", "Unknown peer")
        local = await db.message_content_local(mid)
        if local is None:
            return _err("edit_message", "Not authorised")

        from federation import edit_message_on_peer
        ok, info = await edit_message_on_peer(peer, mid, session["user_id"], text, image)
        if not ok:
            return _err("edit_message", info.get("reason") or "Could not edit message on remote server")

        if info.get("edit_mode") == "history":
            await db.message_edit_history_add(mid, local.get("text"), local.get("image"), info["edited_at"])
        await db.message_content_update(mid, text, image)
        return _ok("edit_message", message={
            "id": mid, "text": text, "image": image, "edited_at": info.get("edited_at"),
        })

    # ── Locally-hosted channel ──
    msg = await db.message_by_id(mid)
    if not msg:
        return _err("edit_message", "Message not found")
    if msg["sender_user_id"] != session["user_id"] or msg.get("sender_peer_id"):
        return _err("edit_message", "Not authorised")

    channel = await db.channel_by_id(msg["channel_id"])
    if not channel:
        return _err("edit_message", "Channel not found")
    mode = channel.get("edit_mode", "off")
    if mode == "off":
        return _err("edit_message", "Editing is turned off in this channel")

    edited_at = datetime.now(timezone.utc)
    if mode == "history":
        await db.message_edit_history_add(mid, msg.get("text"), msg.get("image"), edited_at)
    await db.message_content_update(mid, text, image)
    await db.message_index_set_edited(mid, edited_at)

    bcast_msg = {
        "id": mid, "channel_id": msg["channel_id"], "text": text, "image": image, "edited_at": edited_at,
    }
    await broadcast_to_channel(msg["channel_id"], {"type": "message_edited", "ok": True, "message": bcast_msg})
    if not channel.get("stream_excluded"):
        await push_to_stream({"type": "stream_message_edited", "ok": True, "message": {
            **bcast_msg, "channel_name": channel["name"], "channel_public": channel["public"],
        }})

    from federation import notify_peers_of_edit
    asyncio.create_task(notify_peers_of_edit(msg["channel_id"], mid, text, image, edited_at))

    return _ok("edit_message", message=bcast_msg)


async def _handle_delete_message(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    mid     = _str_uuid(data.get("id"))
    peer_id = _str_uuid(data.get("peer_id"))
    cid     = _str_uuid(data.get("channel_id"))
    if not mid:
        return _err("delete_message", "Missing id")

    if peer_id:
        # Local sender deleting their own message from a remote channel.
        if not cid:
            return _err("delete_message", "Missing channel_id")
        peer = await db.peer_by_id(peer_id)
        if not peer:
            return _err("delete_message", "Unknown peer")
        if not await db.message_content_local(mid):
            return _err("delete_message", "Not authorised")
        from federation import delete_message_on_peer
        ok, info = await delete_message_on_peer(peer, cid, mid, session["user_id"])
        if not ok:
            return _err("delete_message", info.get("reason") or "Could not delete message on remote server")
        await db.message_delete(mid)
        return _ok("delete_message", id=mid)

    msg = await db.message_by_id(mid)
    if not msg:
        return _err("delete_message", "Message not found")

    channel = await db.channel_by_id(msg["channel_id"])
    if not channel:
        return _err("delete_message", "Channel not found")

    is_own    = msg["sender_user_id"] == session["user_id"] and not msg.get("sender_peer_id")
    can_manage = await _can_manage_channel(channel, session["user_id"])
    if not is_own and not can_manage:
        return _err("delete_message", "Not authorised")

    await db.message_delete(mid)

    payload = {"type": "message_deleted", "ok": True, "id": str(mid),
               "channel_id": str(msg["channel_id"])}
    await broadcast_to_channel(msg["channel_id"], payload)
    if not channel.get("stream_excluded"):
        await push_to_stream({**payload, "type": "stream_message_deleted"})

    from federation import notify_peers_of_deletion
    asyncio.create_task(notify_peers_of_deletion(msg["channel_id"], mid))
    return _ok("delete_message", id=mid)


async def _handle_toggle_reaction(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    mid     = _str_uuid(data.get("id"))
    peer_id = _str_uuid(data.get("peer_id"))
    emoji   = _require_str(data, "emoji", 16)
    if not mid:
        return _err("toggle_reaction", "Missing id")
    if not emoji:
        return _err("toggle_reaction", "Missing emoji")

    if peer_id:
        # Message lives in a channel hosted on another server — ask the host.
        peer = await db.peer_by_id(peer_id)
        if not peer:
            return _err("toggle_reaction", "Unknown peer")
        from federation import toggle_reaction_on_peer
        ok, info = await toggle_reaction_on_peer(peer, data.get("channel_id"), mid, session["user_id"], emoji)
        if not ok:
            return _err("toggle_reaction", info.get("reason") or "Could not react on remote server")
        return _ok("toggle_reaction", id=mid, reactions=info.get("reactions", []))

    msg = await db.message_by_id(mid)
    if not msg:
        return _err("toggle_reaction", "Message not found")

    channel, err = await _check_channel_access(msg["channel_id"], session["user_id"])
    if err:
        return _err("toggle_reaction", err)

    if channel["public"] and await db.is_banned(channel["id"], session["user_id"]):
        return _err("toggle_reaction", "You are banned from this channel")

    if not channel.get("allow_reactions", True):
        return _err("toggle_reaction", "Reactions are turned off in this channel")

    await db.reaction_toggle(mid, session["user_id"], None, emoji)
    reactions = await db.reactions_for_message(mid)

    payload = {"type": "reaction_updated", "ok": True, "id": str(mid),
               "channel_id": str(channel["id"]), "reactions": reactions}
    await broadcast_to_channel(channel["id"], payload)

    from federation import notify_peers_of_reaction
    asyncio.create_task(notify_peers_of_reaction(channel["id"], mid, reactions))
    return _ok("toggle_reaction", id=mid, reactions=reactions)


async def _handle_read_message_history(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    mid = _str_uuid(data.get("id"))
    if not mid:
        return _err("read_message_history", "Missing id")
    msg = await db.message_by_id(mid)
    if not msg:
        return _err("read_message_history", "Message not found")
    channel, err = await _check_channel_access(msg["channel_id"], session["user_id"])
    if err:
        return _err("read_message_history", err)
    history = await db.message_edit_history_for(mid)
    return _ok("read_message_history", history=history)


async def _handle_fetch_remote_message_history(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    msg_id       = _str_uuid(data.get("message_id"))
    peer_id      = _str_uuid(data.get("peer_id"))
    peer_address = _require_str(data, "peer_address", 256)

    if not msg_id:
        return _err("fetch_remote_message_history", "Missing message_id")

    local = await db.message_content_local(msg_id)
    if local is not None:
        history = await db.message_edit_history_for(msg_id)
        return _ok("fetch_remote_message_history", history=history)

    peer = None
    if peer_address:
        peer = await db.peer_by_address(peer_address)
    if not peer and peer_id:
        peer = await db.peer_by_id(peer_id)
    if not peer:
        return _err("fetch_remote_message_history", "Unknown peer")

    from federation import fetch_message_history
    history = await fetch_message_history(peer, msg_id)
    if history is None:
        return _err("fetch_remote_message_history", "Unavailable")
    return _ok("fetch_remote_message_history", history=history)


async def _handle_read_members(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("read_members", "Missing channel_id")

    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("read_members", "Channel not found")

    is_creator = channel["created_by"] == session["user_id"]
    can_manage = is_creator or await _can_manage_channel(channel, session["user_id"])
    if not can_manage and not await db.is_member(cid, session["user_id"]):
        return _err("read_members", "Not authorised")

    members    = await db.members_of(cid)
    for m in members:
        m["id"] = m["user_id"]
        # Proxy relative avatar paths for remote members
        peer_addr = m.get("peer_address")
        if peer_addr and m.get("avatar") and not str(m["avatar"]).startswith(("http", "/")):
            base = peer_addr.replace("wss://", "https://").replace("ws://", "http://")
            m["avatar"] = f"{config.BASE_PATH}/proxy_img?url={base}/img/{m['avatar']}"

    if not can_manage:
        # Read-only viewers don't need the "not members" list — that's an editing affordance.
        return _ok("read_members", members=members, non_members=[], can_manage=False, is_creator=False)

    if channel["public"]:
        # Public channels: everyone can read/post already, so member-add/remove is meaningless.
        # Return all local users annotated with moderator status for the creator to manage.
        all_users = await db.users_with_moderator_status(cid)
        for u in all_users:
            u["id"] = u["id"] if "id" in u else u.get("user_id")
        return _ok("read_members", members=all_users, non_members=[], can_manage=True,
                   is_creator=is_creator, is_public=True)

    member_ids = {
        (str(m["user_id"]), str(m["peer_id"]) if m.get("peer_id") else None)
        for m in members
    }

    local_users = await db.users_all()

    from federation import get_peer_users
    peers        = await db.peers_all()
    remote_users = []
    for peer_list in await asyncio.gather(*[get_peer_users(p) for p in peers]):
        remote_users.extend(peer_list)

    all_users   = local_users + remote_users
    non_members = [
        u for u in all_users
        if (str(u["id"]), str(u["peer_id"]) if u.get("peer_id") else None) not in member_ids
    ]

    return _ok("read_members", members=members, non_members=non_members, can_manage=True, is_creator=is_creator)


async def _handle_set_member(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    cid       = _str_uuid(data.get("channel_id"))
    uid       = _str_uuid(data.get("user_id"))
    pid       = _str_uuid(data.get("peer_id"))      # None for local users
    is_member = bool(data.get("is_member", True))

    if not cid or not uid:
        return _err("set_member", "Missing channel_id or user_id")

    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("set_member", "Channel not found")

    if not await _can_manage_channel(channel, session["user_id"]):
        return _err("set_member", "Not authorised")

    if is_member:
        await db.member_add(cid, uid, pid)
        # Cache remote user's profile so members_of can show their name
        if pid:
            name   = _require_str(data, "name", 128)
            avatar = _require_str(data, "avatar", 512)
            if name:
                await db.user_cache_upsert(uid, pid, name, avatar)
    else:
        await db.member_remove(cid, uid)

    return _ok("set_member", channel_id=cid, user_id=uid, peer_id=pid, is_member=is_member)


async def _handle_set_moderator(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    cid    = _str_uuid(data.get("channel_id"))
    uid    = _str_uuid(data.get("user_id"))
    is_mod = bool(data.get("is_moderator", True))

    if not cid or not uid:
        return _err("set_moderator", "Missing channel_id or user_id")

    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("set_moderator", "Channel not found")

    # Only the channel's actual creator appoints/revokes moderators —
    # moderators can't create more moderators.
    if channel["created_by"] != session["user_id"]:
        return _err("set_moderator", "Not authorised")

    if not channel["public"] and not await db.is_member(cid, uid):
        return _err("set_moderator", "User must be a member first")

    # Ensure a channel_members row exists before setting the flag — needed for
    # public channels where users aren't added explicitly as members.
    if is_mod:
        await db.member_add(cid, uid)

    await db.member_set_moderator(cid, uid, is_mod)
    return _ok("set_moderator", channel_id=cid, user_id=uid, is_moderator=is_mod)


async def _handle_read_bans(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("read_bans", "Missing channel_id")
    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("read_bans", "Channel not found")
    can_manage = await _can_manage_channel(channel, session["user_id"])

    banned = await db.channel_bans_for(cid)

    if not can_manage:
        # Anyone can see who's banned from a public channel — only the
        # creator gets the participant list needed to ban someone new.
        return _ok("read_bans", banned=banned, not_banned=[], can_manage=False)

    banned_ids   = {str(b["user_id"]) for b in banned}
    # Participants = users who sent ≥1 message; exclude the requester (can't ban yourself)
    participants = await db.channel_participants(cid)
    my_id        = str(session["user_id"])
    not_banned   = [
        p for p in participants
        if str(p["user_id"]) not in banned_ids and str(p["user_id"]) != my_id
    ]
    return _ok("read_bans", banned=banned, not_banned=not_banned, can_manage=True)


async def _handle_set_ban(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    cid      = _str_uuid(data.get("channel_id"))
    uid      = _str_uuid(data.get("user_id"))
    pid      = _str_uuid(data.get("peer_id"))
    blocked  = bool(data.get("blocked", True))
    if not cid or not uid:
        return _err("set_ban", "Missing channel_id or user_id")
    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("set_ban", "Channel not found")
    if not await _can_manage_channel(channel, session["user_id"]):
        return _err("set_ban", "Not authorised")
    if blocked:
        await db.channel_ban_add(cid, uid, pid)
    else:
        await db.channel_ban_remove(cid, uid)
    return _ok("set_ban", channel_id=cid, user_id=uid, banned=blocked)


async def _handle_subscribe(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("subscribe", "Missing channel_id")
    _subscribe_to_channel(cid, ws)
    return _ok("subscribe", channel_id=cid)


async def _handle_unsubscribe_all(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    _unsubscribe_all(ws)
    return _ok("unsubscribe_all")


def _detect_address() -> str:
    import socket as _socket
    try:
        s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
    except Exception:
        ip = "localhost"
    return f"wss://{ip}:{config.PORT}{config.BASE_PATH}"


def _cert_info(cert_path: str) -> dict:
    """Parse a PEM cert file and return display info. Returns {} on error."""
    try:
        from cryptography import x509 as cx
        data = cx.load_pem_x509_certificate(open(cert_path, "rb").read())
        cn          = data.subject.get_attributes_for_oid(cx.oid.NameOID.COMMON_NAME)
        cn_str      = cn[0].value if cn else ""
        self_signed = data.subject == data.issuer
        expires     = data.not_valid_after_utc.strftime("%Y-%m-%d")
        valid_days  = (data.not_valid_after_utc - data.not_valid_before_utc).days
        return {"cn": cn_str, "self_signed": self_signed, "expires": expires, "valid_days": valid_days}
    except Exception:
        return {}


async def _handle_read_cert_config(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("read_cert_config", "Not authorised")
    return _ok("read_cert_config",
               cert_path=config.SSL_CERT,
               key_path=config.SSL_KEY,
               info=_cert_info(config.SSL_CERT))


async def _handle_set_cert_config(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("set_cert_config", "Not authorised")
    cert_path = _require_str(data, "cert_path", 512)
    key_path  = _require_str(data, "key_path",  512)
    if not cert_path or not key_path:
        return _err("set_cert_config", "Cert path and key path required")
    import pathlib, ssl as _ssl
    if not pathlib.Path(cert_path).exists():
        return _err("set_cert_config", f"Cert file not found: {cert_path}")
    if not pathlib.Path(key_path).exists():
        return _err("set_cert_config", f"Key file not found: {key_path}")
    try:
        ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert_path, key_path)
    except _ssl.SSLError as e:
        return _err("set_cert_config", f"Invalid cert/key pair: {e}")

    # Expiry and domain checks
    domain_warning = None
    try:
        from cryptography import x509 as cx
        from datetime import datetime, timezone as _tz
        cert_data = cx.load_pem_x509_certificate(pathlib.Path(cert_path).read_bytes())

        if cert_data.not_valid_after_utc < datetime.now(_tz.utc):
            return _err("set_cert_config", "Certificate has expired")

        peer_address = await db.setting_get("peer_address") or ""
        if peer_address:
            import urllib.parse
            host = urllib.parse.urlparse(
                peer_address.replace("wss://", "https://").replace("ws://", "http://")
            ).hostname or ""
            if host:
                try:
                    san = cert_data.extensions.get_extension_for_class(cx.SubjectAlternativeName)
                    san_names = san.value.get_values_for_type(cx.DNSName)
                except Exception:
                    san_names = []
                if not san_names:
                    cn_attrs = cert_data.subject.get_attributes_for_oid(cx.oid.NameOID.COMMON_NAME)
                    san_names = [cn_attrs[0].value] if cn_attrs else []
                matched = any(
                    n == host or (n.startswith("*.") and host.endswith(n[1:]) and "." in host)
                    for n in san_names
                )
                if not matched:
                    domain_warning = (
                        f"Domain mismatch: cert is for {', '.join(san_names)}, "
                        f"but peer address host is {host}"
                    )
    except Exception as e:
        log.warning("set_cert_config cert parse: %s", e)

    # Create a succession record so offline peers can self-heal after rotation
    if pathlib.Path(config.SSL_CERT).exists():
        import ssl_manager as _sm
        record = _sm.sign_succession(config.SSL_KEY, config.SSL_CERT, cert_path)
        if record:
            db_config.succession_add(record)
            log.info("Cert succession record created: %s → %s",
                     record["old_fingerprint"][:12], record["new_fingerprint"][:12])

    db_config.cfg_set("ssl_cert", cert_path)
    db_config.cfg_set("ssl_key",  key_path)
    return _ok("set_cert_config", info=_cert_info(cert_path), domain_warning=domain_warning)


async def _handle_renew_cert(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("renew_cert", "Not authorised")

    info = _cert_info(config.SSL_CERT)
    if not info.get("self_signed"):
        return _err("renew_cert", "Only self-signed certificates can be renewed here")

    import pathlib, ssl_manager as _sm
    from cryptography import x509 as cx
    from cryptography.hazmat.primitives import serialization

    cert_path = pathlib.Path(config.SSL_CERT)
    key_path  = pathlib.Path(config.SSL_KEY)

    # Read old cert + key into memory BEFORE overwriting
    try:
        old_cert_der = cx.load_pem_x509_certificate(cert_path.read_bytes()) \
                         .public_bytes(serialization.Encoding.DER)
        old_key_pem  = key_path.read_bytes()
        cn_attrs = cx.load_der_x509_certificate(old_cert_der) \
                     .subject.get_attributes_for_oid(cx.oid.NameOID.COMMON_NAME)
        hostname = cn_attrs[0].value if cn_attrs else "localhost"
    except Exception as e:
        return _err("renew_cert", f"Could not read current cert: {e}")

    # Generate new self-signed cert (overwrites files)
    try:
        _sm._generate_self_signed(cert_path, key_path, hostname)
    except Exception as e:
        return _err("renew_cert", f"Could not generate new cert: {e}")

    # Sign succession with old key (still in memory)
    new_cert_der = cx.load_pem_x509_certificate(cert_path.read_bytes()) \
                     .public_bytes(serialization.Encoding.DER)
    record = _sm.sign_succession_bytes(old_key_pem, old_cert_der, new_cert_der)
    if record:
        db_config.succession_add(record)
        log.info("Self-signed cert renewed; succession %s → %s",
                 record["old_fingerprint"][:12], record["new_fingerprint"][:12])

    # Hot-reload — no restart needed
    _sm.reload_cert_chain(str(cert_path), str(key_path))

    return _ok("renew_cert", info=_cert_info(str(cert_path)))


async def _handle_reload_cert(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("reload_cert", "Not authorised")

    import pathlib, ssl as _ssl
    cert_path = config.SSL_CERT
    key_path  = config.SSL_KEY
    if not cert_path or not key_path:
        return _err("reload_cert", "No certificate configured")
    if not pathlib.Path(cert_path).exists():
        return _err("reload_cert", f"Cert file not found: {cert_path}")
    if not pathlib.Path(key_path).exists():
        return _err("reload_cert", f"Key file not found: {key_path}")
    try:
        ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert_path, key_path)
    except _ssl.SSLError as e:
        return _err("reload_cert", f"Invalid cert/key pair: {e}")

    import ssl_manager as _sm
    _sm.reload_cert_chain(cert_path, key_path)
    return _ok("reload_cert", info=_cert_info(cert_path))


def _disk_info(path: str) -> dict | None:
    try:
        import shutil
        u = shutil.disk_usage(path)
        return {"free": u.free, "total": u.total}
    except Exception:
        return None


async def _handle_read_upload_config(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("read_upload_config", "Not authorised")
    return _ok("read_upload_config", upload_dir=config.UPLOAD_DIR, disk=_disk_info(config.UPLOAD_DIR))


def _migrate_files(old_dir: str, new_dir: str, mode: str):
    """Sync: copy or move files from old_dir to new_dir. Runs in executor."""
    import shutil, os
    os.makedirs(new_dir, exist_ok=True)
    for fname in os.listdir(old_dir):
        src = os.path.join(old_dir, fname)
        dst = os.path.join(new_dir, fname)
        if not os.path.isfile(src):
            continue
        if mode == "copy":
            shutil.copy2(src, dst)
        elif mode == "move":
            shutil.move(src, dst)


async def _handle_set_upload_config(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("set_upload_config", "Not authorised")
    upload_dir = _require_str(data, "upload_dir", 512)
    if not upload_dir:
        return _err("set_upload_config", "Path required")
    migrate = data.get("migrate", "none")  # "none" | "copy" | "move"

    import pathlib as _pl
    try:
        _pl.Path(upload_dir).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        return _err("set_upload_config", f"Cannot create directory: {e}")

    if migrate in ("copy", "move") and upload_dir != config.UPLOAD_DIR:
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, _migrate_files, config.UPLOAD_DIR, upload_dir, migrate
            )
        except Exception as e:
            return _err("set_upload_config", f"Migration failed: {e}")

    db_config.cfg_set("upload_dir", upload_dir)
    return _ok("set_upload_config", upload_dir=upload_dir)


async def _handle_read_db_config(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("read_db_config", "Not authorised")
    dsn = config.DB_DSN
    db_type = "sqlite" if dsn.startswith("sqlite") else "postgres"
    sqlite_path = dsn.removeprefix("sqlite://") if db_type == "sqlite" else ""
    return _ok("read_db_config", dsn=dsn, db_type=db_type, sqlite_path=sqlite_path)


async def _handle_set_db_config(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("set_db_config", "Not authorised")
    db_type = data.get("db_type", "sqlite")
    if db_type == "sqlite":
        path = _require_str(data, "path", 512)
        if not path:
            return _err("set_db_config", "Path required")
        dsn = "sqlite://" + path
    else:
        dsn = _require_str(data, "dsn", 512)
        if not dsn or not dsn.startswith("postgresql"):
            return _err("set_db_config", "Connection string must start with postgresql://")
    db_config.cfg_set("db_dsn", dsn)
    return _ok("set_db_config", dsn=dsn)


async def _handle_read_peers(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    from federation import get_connected_peers, get_peer_statuses
    peers        = await db.peers_all()
    connected    = get_connected_peers()
    statuses     = get_peer_statuses()
    for p in peers:
        addr   = p.get("address", "")
        sess   = statuses.get(addr, {})
        p["connected"]      = addr in connected
        p["session_status"] = sess.get("status", "disconnected")
        p["session_reason"] = sess.get("reason", "")
    peer_name    = await db.setting_get("peer_name") or ""
    peer_address = await db.setting_get("peer_address") or _detect_address()
    policy       = await db.setting_get("peer_policy") or "open"
    return _ok("read_peers", peers=peers, peer_name=peer_name,
               peer_address=peer_address, policy=policy)


async def _handle_set_peer_policy(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("set_peer_policy", "Not authorised")
    policy = data.get("policy", "open")
    if policy not in ("open", "approval", "closed"):
        return _err("set_peer_policy", "Invalid policy")
    await db.setting_set("peer_policy", policy)
    return _ok("set_peer_policy", policy=policy)


async def _handle_approve_peer(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("approve_peer", "Not authorised")
    pid    = _str_uuid(data.get("id"))
    status = data.get("status")
    if not pid or status not in ("approved", "blocked"):
        return _err("approve_peer", "Missing id or invalid status")
    await db.peer_set_status(pid, status)
    if status == "approved":
        await push_to_stream({"type": "peer_added", "ok": True})
    return _ok("approve_peer", id=pid, status=status)


async def _handle_set_peer_name(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("set_peer_name", "Not authorised")
    name = _require_str(data, "name", 128)
    if not name:
        return _err("set_peer_name", "Name required")
    await db.setting_set("peer_name", name)
    return _ok("set_peer_name", name=name)


async def _handle_set_peer_address(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("set_peer_address", "Not authorised")
    address = _require_str(data, "address", 256)
    if not address or not address.startswith(("wss://", "ws://")):
        return _err("set_peer_address", "Address must start with wss://")
    await db.setting_set("peer_address", address)
    # Derive and persist base path so it's available at next startup
    import urllib.parse as _up
    _path = _up.urlparse(address.replace("wss://", "https://").replace("ws://", "http://")).path.rstrip("/")
    db_config.cfg_set("base_path", _path)
    return _ok("set_peer_address", address=address, restart_required=bool(_path != config.BASE_PATH))


async def _handle_add_peer(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("add_peer", "Not authorised")
    address = _require_str(data, "address", 256)
    if not address or not address.startswith(("wss://", "ws://")):
        return _err("add_peer", "Address must start with wss://")

    if await db.peer_by_address(address):
        return _err("add_peer", "Already connected to this server")

    from federation import connect_peer, get_peer_channels
    peer, err = await connect_peer(address)
    if err and not peer:
        return _err("add_peer", err)

    peer["channels"] = await get_peer_channels(peer) if not err else []
    if not err:
        await push_to_stream({"type": "peer_added", "ok": True})
    return _ok("add_peer", peer=peer, pending=bool(err))


async def _handle_fetch_remote_message(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    msg_id       = _str_uuid(data.get("message_id"))
    peer_id      = _str_uuid(data.get("peer_id"))
    peer_address = _require_str(data, "peer_address", 256)

    if not msg_id:
        return _err("fetch_remote_message", "Missing message_id")

    # Content stored locally (we are the origin server for this message).
    # Skip sentinel records ("📷") — those mean "image exists on peer", fetch for real.
    local = await db.message_content_local(msg_id)
    if local and local.get("image") != "📷":
        return _ok("fetch_remote_message", message=local)

    # Look up peer: by current address first, then by stored peer_id as fallback
    # (handles renamed peers where stored address is stale)
    peer = None
    if peer_address:
        peer = await db.peer_by_address(peer_address)
    if not peer and peer_id:
        peer = await db.peer_by_id(peer_id)

    if not peer:
        return _err("fetch_remote_message", "Unknown peer")

    from federation import fetch_message_content
    content = await fetch_message_content(peer, msg_id)
    if content is None:
        return _err("fetch_remote_message", "Unavailable")

    # Convert relative image path to a proxied URL served by the local server.
    # Direct cross-origin URLs fail when the peer uses a self-signed certificate.
    if content.get("image") and not str(content["image"]).startswith("http"):
        base      = peer["address"].replace("wss://", "https://").replace("ws://", "http://")
        remote_url = f"{base}/img/{content['image']}"
        content["image"] = f"{config.BASE_PATH}/proxy_img?url={remote_url}"

    return _ok("fetch_remote_message", message=content)


async def _handle_delete_peer(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("delete_peer", "Not authorised")
    pid = _str_uuid(data.get("id"))
    if not pid:
        return _err("delete_peer", "Missing id")
    peer = await db.peer_by_id(pid)
    if not peer:
        return _err("delete_peer", "Peer not found")
    from federation import drop_peer_session
    if peer.get("status") == "blocked":
        # Already blocked — permanently remove
        await db.peer_delete(pid)
        await drop_peer_session(peer.get("address", ""))
        return _ok("delete_peer", id=pid, removed=True)
    else:
        # Soft delete: block so they can no longer connect
        await db.peer_block(pid)
        await drop_peer_session(peer.get("address", ""))
        return _ok("delete_peer", id=pid, removed=False)


async def _handle_read_update_config(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("read_update_config", "Not authorised")

    auto_update = db_config.cfg_get("auto_update") == "1"

    import asyncio as _aio, subprocess as _sub
    from pathlib import Path as _Path
    repo_dir = str(_Path(__file__).parent.parent)

    async def _git(*args):
        try:
            r = await _aio.get_event_loop().run_in_executor(
                None, lambda: _sub.run(["git", "-C", repo_dir] + list(args),
                                       capture_output=True, text=True, timeout=10))
            return r.stdout.strip()
        except Exception:
            return ""

    current = await _git("describe", "--tags", "--exact-match") \
           or await _git("describe", "--tags") \
           or await _git("rev-parse", "--short", "HEAD") \
           or "unknown"

    return _ok("read_update_config", auto_update=auto_update, current_version=current)


async def _handle_set_update_config(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("set_update_config", "Not authorised")

    auto_update = bool(data.get("auto_update", False))
    db_config.cfg_set("auto_update", "1" if auto_update else "0")
    return _ok("set_update_config", auto_update=auto_update)


async def _handle_check_update(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("check_update", "Not authorised")

    import asyncio as _aio, subprocess as _sub
    from pathlib import Path as _Path
    repo_dir = str(_Path(__file__).parent.parent)

    async def _git(*args, timeout=30):
        try:
            r = await _aio.get_event_loop().run_in_executor(
                None, lambda: _sub.run(["git", "-C", repo_dir] + list(args),
                                       capture_output=True, text=True, timeout=timeout))
            return r.stdout.strip()
        except Exception:
            return ""

    await _git("fetch", "--tags", "--quiet", timeout=30)

    current = await _git("describe", "--tags", "--exact-match") \
           or await _git("describe", "--tags") \
           or await _git("rev-parse", "--short", "HEAD") \
           or "unknown"

    tags = await _git("tag", "--sort=-version:refname")
    latest = tags.splitlines()[0] if tags else ""

    if not latest:
        return _ok("check_update", current=current, latest="", update_available=False,
                   message="No release tags found in repository.")

    behind_str = await _git("rev-list", "--count", f"HEAD..refs/tags/{latest}")
    try:
        behind = int(behind_str)
    except (ValueError, TypeError):
        behind = 0

    update_available = behind > 0
    return _ok("check_update", current=current, latest=latest,
               update_available=update_available, behind=behind)


async def _handle_apply_update(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("apply_update", "Not authorised")

    import subprocess as _sub, sys as _sys, os as _os
    from pathlib import Path as _Path
    repo_dir = str(_Path(__file__).parent.parent)

    # Enable auto_update temporarily so update.sh proceeds even if the setting is off
    db_config.cfg_set("auto_update", "1")

    script = _Path(repo_dir) / "update.sh"
    if not script.exists():
        return _err("apply_update", "update.sh not found")

    # Run detached so the script survives if systemctl restarts our process
    try:
        _sub.Popen(
            ["bash", str(script)],
            stdout=_sub.DEVNULL, stderr=_sub.DEVNULL,
            close_fds=True, start_new_session=True,
            cwd=repo_dir,
        )
    except Exception as e:
        return _err("apply_update", f"Could not start update: {e}")

    return _ok("apply_update", message="Update started. The server will restart if a new version is found.")


async def _handle_resolve_channel_link(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    """Resolve a pasted 'https://<host>/join/<channel_id>' link to an actionable
    next step — open locally, open via a known peer, show the owner to ask for
    access, or (if not yet federated) offer to connect / draft a request."""
    raw_url = _require_str(data, "url", 512)
    if not raw_url or "/join/" not in raw_url:
        return _err("resolve_channel_link", "That doesn't look like a Messages chat link")

    base_part, _, tail = raw_url.partition("/join/")
    channel_id_part = tail.split("?")[0].split("#")[0].rstrip("/")
    cid = _str_uuid(channel_id_part)
    if not cid:
        return _err("resolve_channel_link", "Invalid channel id in link")

    import urllib.parse as _up
    parsed = _up.urlparse(base_part)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return _err("resolve_channel_link", "Invalid link")

    host_https = base_part.rstrip("/")
    host_wss   = host_https.replace("https://", "wss://").replace("http://", "ws://")

    our_address = await db.setting_get("peer_address") or _detect_address()
    our_https   = our_address.replace("wss://", "https://").replace("ws://", "http://").rstrip("/")

    if host_https == our_https or host_wss == our_address.rstrip("/"):
        return _ok("resolve_channel_link", kind="local", channel_id=cid)

    peer = await db.peer_by_address(host_wss)

    if peer and peer.get("status") == "approved":
        channel, messages = await get_remote_messages(peer, cid, session["user_id"])
        if channel and channel.get("access_denied"):
            return _ok("resolve_channel_link", kind="denied", peer_id=peer["id"],
                       channel_name=channel.get("name"), owner=channel.get("owner"))
        if channel:
            return _ok("resolve_channel_link", kind="remote", peer_id=peer["id"], channel_id=cid)
        return _err("resolve_channel_link", "Could not reach that server, or the chat no longer exists")

    owner_id = await db.setting_get("owner_id")
    is_owner = str(session["user_id"]) == owner_id
    return _ok("resolve_channel_link", kind="not_connected", host=host_wss, is_owner=is_owner,
               owner_id=(None if is_owner else owner_id))


async def _handle_read_advanced_config(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("read_advanced_config", "Not authorised")
    return _ok("read_advanced_config",
        # Network (restart required)
        base_path            = config.BASE_PATH,
        host                 = config.HOST,
        port                 = config.PORT,
        port_http            = config.PORT_HTTP,
        port_default         = 443,
        port_http_default    = 80,
        # Rate limiting (hot-apply)
        rate_limit_login     = config.RATE_LIMIT_LOGIN,
        rate_limit_messages  = config.RATE_LIMIT_MESSAGES,
        trusted_proxies      = ",".join(sorted(config.TRUSTED_PROXIES)),
        # Federation (hot-apply)
        peer_connect_timeout = config.PEER_CONNECT_TIMEOUT,
        # Sessions (restart required)
        session_max_age_days = config.SESSION_MAX_AGE // 86400,
        # Uploads (restart required)
        upload_max_mb        = config.UPLOAD_MAX_MB,
    )


async def _handle_set_advanced_config(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("set_advanced_config", "Not authorised")

    needs_restart = False

    def _int(key, lo, hi):
        v = data.get(key)
        if v is None:
            return None
        try:
            return max(lo, min(hi, int(v)))
        except (ValueError, TypeError):
            return None

    # ── Hot-apply: rate limiting ──────────────────────────────────────────
    rl_login = _int("rate_limit_login", 1, 1000)
    if rl_login is not None:
        config.RATE_LIMIT_LOGIN = rl_login
        db_config.cfg_set("rate_limit_login", str(rl_login))

    rl_msg = _int("rate_limit_messages", 1, 10000)
    if rl_msg is not None:
        config.RATE_LIMIT_MESSAGES = rl_msg
        db_config.cfg_set("rate_limit_messages", str(rl_msg))

    trusted = data.get("trusted_proxies")
    if trusted is not None:
        cleaned = ",".join(filter(None, (p.strip() for p in str(trusted).split(","))))
        config.TRUSTED_PROXIES = set(filter(None, cleaned.split(",")))
        db_config.cfg_set("trusted_proxies", cleaned)

    # ── Hot-apply: federation ─────────────────────────────────────────────
    timeout = _int("peer_connect_timeout", 1, 120)
    if timeout is not None:
        config.PEER_CONNECT_TIMEOUT = timeout
        db_config.cfg_set("peer_connect_timeout", str(timeout))

    # ── Restart required ──────────────────────────────────────────────────
    host = data.get("host")
    if host is not None:
        h = str(host).strip()
        db_config.cfg_set("host", h)
        needs_restart = True

    port = _int("port", 1, 65535)
    if port is not None:
        db_config.cfg_set("port", str(port))
        needs_restart = True

    port_http = _int("port_http", 0, 65535)
    if port_http is not None:
        db_config.cfg_set("port_http", str(port_http))
        needs_restart = True

    session_days = _int("session_max_age_days", 1, 3650)
    if session_days is not None:
        secs = session_days * 86400
        if secs != config.SESSION_MAX_AGE:
            db_config.cfg_set("session_max_age", str(secs))
            needs_restart = True

    upload_mb = _int("upload_max_mb", 1, 500)
    if upload_mb is not None:
        if upload_mb != config.UPLOAD_MAX_MB:
            db_config.cfg_set("upload_max_mb", str(upload_mb))
            needs_restart = True

    return _ok("set_advanced_config", needs_restart=needs_restart)


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_HANDLERS = {
    "ping":             _handle_login,           # keep-alive / session check
    "session":          _handle_login,
    "logout":           _handle_logout,
    "read_stream":           _handle_read_stream,
    "toggle_stream_channel": _handle_toggle_stream_channel,
    "set_channel_allow_replies":   _handle_set_channel_allow_replies,
    "set_channel_public":          _handle_set_channel_public,
    "set_channel_post_restricted": _handle_set_channel_post_restricted,
    "set_channel_restrict_replies": _handle_set_channel_restrict_replies,
    "set_channel_allow_images":    _handle_set_channel_allow_images,
    "set_channel_allow_reactions": _handle_set_channel_allow_reactions,
    "set_channel_allow_markdown":  _handle_set_channel_allow_markdown,
    "set_channel_edit_mode":       _handle_set_channel_edit_mode,
    "set_channel_description":     _handle_set_channel_description,
    "read_channels":    _handle_read_channels,
    "read_users":       _handle_read_users,
    "read_user_messages":    _handle_read_user_messages,
    "read_user":             _handle_read_user,
    "set_display_name":      _handle_set_display_name,
    "set_channel_icon":      _handle_set_channel_icon,
    "read_user_settings":    _handle_read_user_settings,
    "set_user_setting":      _handle_set_user_setting,
    "read_profiles":         _handle_read_profiles,
    "delete_user":           _handle_delete_user,
    "start_chat":       _handle_start_chat,
    "create_channel":   _handle_create_channel,
    "delete_channel":   _handle_delete_channel,
    "read_channel":     _handle_read_channel,
    "message":          _handle_message,
    "read_members":     _handle_read_members,
    "set_member":       _handle_set_member,
    "set_moderator":    _handle_set_moderator,
    "read_bans":      _handle_read_bans,
    "set_ban":        _handle_set_ban,
    "subscribe":        _handle_subscribe,
    "unsubscribe_all":  _handle_unsubscribe_all,
    "renew_cert":       _handle_renew_cert,
    "reload_cert":      _handle_reload_cert,
    "read_cert_config": _handle_read_cert_config,
    "set_cert_config":  _handle_set_cert_config,
    "read_upload_config": _handle_read_upload_config,
    "set_upload_config":  _handle_set_upload_config,
    "read_db_config":        _handle_read_db_config,
    "set_db_config":         _handle_set_db_config,
    "read_advanced_config":  _handle_read_advanced_config,
    "set_advanced_config":   _handle_set_advanced_config,
    "read_update_config":    _handle_read_update_config,
    "set_update_config":     _handle_set_update_config,
    "check_update":          _handle_check_update,
    "apply_update":          _handle_apply_update,
    "read_peers":       _handle_read_peers,
    "set_peer_policy":  _handle_set_peer_policy,
    "approve_peer":     _handle_approve_peer,
    "set_peer_name":    _handle_set_peer_name,
    "set_peer_address": _handle_set_peer_address,
    "add_peer":         _handle_add_peer,
    "delete_peer":           _handle_delete_peer,
    "fetch_remote_message":  _handle_fetch_remote_message,
    "edit_message":              _handle_edit_message,
    "delete_message":            _handle_delete_message,
    "toggle_reaction":           _handle_toggle_reaction,
    "mark_channel_read":         _handle_mark_channel_read,
    "read_message_history":      _handle_read_message_history,
    "fetch_remote_message_history": _handle_fetch_remote_message_history,
    "resolve_channel_link":  _handle_resolve_channel_link,
}


# ---------------------------------------------------------------------------
# WebSocket entry point
# ---------------------------------------------------------------------------

async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    session = await auth.session_from_request(request)

    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    if not session:
        await ws.send_str(_dumps(_err("auth", "Not authenticated")))
        await ws.close()
        return ws

    # Attach the token so logout handler can invalidate it
    session["_token"] = request.cookies.get(config.SESSION_COOKIE)

    log.info("WS connected: user=%s", session["name"])
    _notification_sockets.add(ws)

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    await ws.send_str(_dumps(_err("parse", "Invalid JSON")))
                    continue

                msg_type = data.get("type", "")
                handler  = _HANDLERS.get(msg_type)

                if not handler:
                    await ws.send_str(_dumps(_err(msg_type, f"Unknown type: {msg_type}")))
                    continue

                try:
                    result = await handler(data, session, ws)
                    if result is not None:
                        await ws.send_str(_dumps(result))
                except Exception as e:
                    log.exception("Error handling WS message type=%s", msg_type)
                    await ws.send_str(_dumps(_err(msg_type, "Internal error")))

            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                break
    finally:
        _unsubscribe_all(ws)
        log.info("WS disconnected: user=%s", session["name"])

    return ws
