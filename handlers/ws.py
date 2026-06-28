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

_channel_sockets: dict[uuid.UUID, set[web.WebSocketResponse]] = defaultdict(set)


def _subscribe_to_channel(channel_id: uuid.UUID, ws: web.WebSocketResponse):
    _channel_sockets[channel_id].add(ws)


def _unsubscribe_from_channel(channel_id: uuid.UUID, ws: web.WebSocketResponse):
    _channel_sockets[channel_id].discard(ws)


def _unsubscribe_all(ws: web.WebSocketResponse):
    for sockets in _channel_sockets.values():
        sockets.discard(ws)


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


async def _handle_read_channels(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    channels  = await db.channels_visible_to(session["user_id"])
    peer_list = await db.peers_all()

    from federation import get_peer_channels
    remote = await asyncio.gather(*[get_peer_channels(p) for p in peer_list])
    for peer, chs in zip(peer_list, remote):
        peer["channels"] = chs

    return _ok("read_channels", channels=channels, peers=peer_list)


async def _handle_read_users(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    users = await db.users_all()
    return _ok("read_users", users=users)


async def _handle_read_user(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    uid = _str_uuid(data.get("id"))
    if not uid:
        return _err("read_user", "Missing id")
    user = await db.user_by_id(uid)
    if not user:
        return _err("read_user", "User not found")
    user.pop("password", None)
    return _ok("read_user", user=user)


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


async def _handle_create_channel(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    name   = _require_str(data, "name", 128)
    public = bool(data.get("public", True))
    if not name:
        return _err("create_channel", "Channel name required")

    channel = await db.channel_create(name, public, session["user_id"])
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

    owner_id = await db.setting_get("owner_id")
    is_owner   = str(session["user_id"]) == owner_id
    is_creator = channel["created_by"] == session["user_id"]
    if not is_owner and not is_creator:
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
        messages = await get_remote_messages(peer, cid, session["user_id"])
        _subscribe_to_channel(cid, ws)   # receive live pushes from the peer
        return _ok("read_channel", messages=messages, remote=True)

    if not cid:
        return _err("read_channel", "Missing id")

    channel, err = await _check_channel_access(cid, session["user_id"])
    if err:
        return _err("read_channel", err)

    _subscribe_to_channel(cid, ws)
    messages = await db.messages_for_channel(cid)
    return _ok("read_channel", channel=channel, messages=messages)


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
        ok = await send_message_to_peer(
            peer, cid, msg_id, session["user_id"], our_address, created,
            sender_name=user["name"] or "",
            sender_avatar=avatar_url,
            parent_id=parent,
        )
        if not ok:
            return _err("message", "Could not deliver message to remote server")

    else:
        # ── Local channel: store index + content here ──
        channel, err = await _check_channel_access(cid, session["user_id"])
        if err:
            return _err("message", err)

        await db.message_index_add(msg_id, cid, session["user_id"], None, parent, created)
        await db.message_content_add(msg_id, text, image)

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
        asyncio.create_task(notify_peers_of_message(
            cid, msg_id, session["user_id"], created,
            sender_name=user["name"] or "",
            sender_avatar=user["avatar"],
        ))
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


async def _handle_read_members(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    cid = _str_uuid(data.get("channel_id"))
    if not cid:
        return _err("read_members", "Missing channel_id")

    channel = await db.channel_by_id(cid)
    if not channel:
        return _err("read_members", "Channel not found")

    owner_id   = await db.setting_get("owner_id")
    is_owner   = str(session["user_id"]) == owner_id
    is_creator = channel["created_by"] == session["user_id"]
    if not is_owner and not is_creator:
        return _err("read_members", "Not authorised")

    members    = await db.members_of(cid)
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

    return _ok("read_members", members=members, non_members=non_members)


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

    owner_id   = await db.setting_get("owner_id")
    is_owner   = str(session["user_id"]) == owner_id
    is_creator = channel["created_by"] == session["user_id"]
    if not is_owner and not is_creator:
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
    return f"wss://{ip}:{config.PORT}"


async def _handle_read_peers(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    peers        = await db.peers_all()
    peer_name    = await db.setting_get("peer_name") or ""
    peer_address = await db.setting_get("peer_address") or _detect_address()
    return _ok("read_peers", peers=peers, peer_name=peer_name, peer_address=peer_address)


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
    return _ok("set_peer_address", address=address)


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
    if err:
        return _err("add_peer", err)

    peer["channels"] = await get_peer_channels(peer)
    return _ok("add_peer", peer=peer)


async def _handle_fetch_remote_message(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    msg_id       = _str_uuid(data.get("message_id"))
    peer_id      = _str_uuid(data.get("peer_id"))
    peer_address = _require_str(data, "peer_address", 256)

    if not msg_id:
        return _err("fetch_remote_message", "Missing message_id")

    # Content stored locally (we are the origin server for this message)
    local = await db.message_content_local(msg_id)
    if local:
        return _ok("fetch_remote_message", message=local)

    # Look up peer by address first (cross-server IDs don't match), then by id
    peer = None
    if peer_address:
        peer = await db.peer_by_address(peer_address)
        if not peer:
            peer = await db.peer_upsert(peer_address, None, None)
    elif peer_id:
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
        content["image"] = f"/proxy_img?url={remote_url}"

    return _ok("fetch_remote_message", message=content)


async def _handle_delete_peer(data: dict, session: dict, ws: web.WebSocketResponse) -> dict:
    owner_id = await db.setting_get("owner_id")
    if str(session["user_id"]) != owner_id:
        return _err("delete_peer", "Not authorised")
    pid = _str_uuid(data.get("id"))
    if not pid:
        return _err("delete_peer", "Missing id")
    await db.peer_delete(pid)
    return _ok("delete_peer", id=pid)


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_HANDLERS = {
    "ping":             _handle_login,           # keep-alive / session check
    "session":          _handle_login,
    "logout":           _handle_logout,
    "read_channels":    _handle_read_channels,
    "read_users":       _handle_read_users,
    "read_user":        _handle_read_user,
    "read_profiles":    _handle_read_profiles,
    "delete_user":      _handle_delete_user,
    "create_channel":   _handle_create_channel,
    "delete_channel":   _handle_delete_channel,
    "read_channel":     _handle_read_channel,
    "message":          _handle_message,
    "read_members":     _handle_read_members,
    "set_member":       _handle_set_member,
    "subscribe":        _handle_subscribe,
    "unsubscribe_all":  _handle_unsubscribe_all,
    "read_peers":       _handle_read_peers,
    "set_peer_name":    _handle_set_peer_name,
    "set_peer_address": _handle_set_peer_address,
    "add_peer":         _handle_add_peer,
    "delete_peer":           _handle_delete_peer,
    "fetch_remote_message":  _handle_fetch_remote_message,
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
