"""
Server-to-server federation.

/peer  WebSocket endpoint — other servers connect here.

Outbound:
  connect_peer(address)       — establish connection and store peer
  get_remote_messages(peer, channel_id, user_id) — fetch message index from peer
  notify_peers_of_message(...)  — tell subscribed peers a new message exists

Trust model: trust-on-first-use (TOFU).
  First connection: accept any cert, record the SHA-256 fingerprint.
  Subsequent connections: require the fingerprint to match.

Peer authentication: each side sends their own address + a shared secret
that is exchanged on first connect and stored in the DB.
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone

import aiohttp
from aiohttp import web

import config
import db
import ssl_manager
from utils import _dumps, _ok, _err, _str_uuid

log = logging.getLogger(__name__)


def _detect_outbound_address() -> str:
    import socket as _socket
    try:
        s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
    except Exception:
        ip = "localhost"
    return f"wss://{ip}:{config.PORT}"


# ---------------------------------------------------------------------------
# Shared HTTP/WS client session (reuses connections)
# ---------------------------------------------------------------------------

_client_session: aiohttp.ClientSession | None = None


def _get_session() -> aiohttp.ClientSession:
    global _client_session
    if _client_session is None or _client_session.closed:
        _client_session = aiohttp.ClientSession()
    return _client_session


async def close_session():
    global _client_session
    if _client_session and not _client_session.closed:
        await _client_session.close()
        _client_session = None


# ---------------------------------------------------------------------------
# Subscriptions: channel_id → set of peer addresses watching it
# ---------------------------------------------------------------------------

_peer_subscriptions: dict[uuid.UUID, set[str]] = {}


def _add_peer_subscription(channel_id: uuid.UUID, peer_address: str):
    _peer_subscriptions.setdefault(channel_id, set()).add(peer_address)


def _remove_peer_subscriptions(peer_address: str):
    for subs in _peer_subscriptions.values():
        subs.discard(peer_address)


# ---------------------------------------------------------------------------
# Outbound: connect to a peer
# ---------------------------------------------------------------------------

async def connect_peer(address: str) -> tuple[dict | None, str | None]:
    """
    Connect to a peer server, perform TOFU fingerprint check,
    exchange names, and store the peer in the DB.
    Returns (peer_dict, error_str).
    """
    existing = await db.peer_by_address(address)
    fingerprint = existing["ssl_fingerprint"] if existing else None

    ssl_ctx = ssl_manager.peer_ssl_context(fingerprint)

    try:
        async with _get_session().ws_connect(
                address + "/peer",
                ssl=ssl_ctx,
                timeout=aiohttp.ClientWSTimeout(ws_close=config.PEER_CONNECT_TIMEOUT),
            ) as ws:
                # Introduce ourselves and get peer details
                our_name    = await db.setting_get("peer_name") or ""
                our_address = await db.setting_get("peer_address") or _detect_outbound_address()
                await ws.send_str(_dumps({
                    "type":         "peer_details",
                    "from_address": our_address,
                    "from_name":    our_name,
                }))
                raw = await asyncio.wait_for(ws.receive_str(), timeout=10)
                details = json.loads(raw)

                peer_name    = details.get("name", "")
                peer_address = details.get("address") or address  # fall back to address we dialled

                # Record the cert fingerprint (TOFU)
                ssl_obj = ws.get_extra_info("ssl_object") if hasattr(ws, "get_extra_info") else None
                fp = None
                if ssl_obj:
                    cert_der = ssl_obj.getpeercert(binary_form=True)
                    if cert_der:
                        fp = ssl_manager.cert_fingerprint(cert_der)
                        if fingerprint and fp != fingerprint:
                            return None, "Certificate fingerprint mismatch — possible MITM"

                peer = await db.peer_upsert(peer_address, peer_name, fp)
                return peer, None

    except asyncio.TimeoutError:
        return None, "Connection timed out"
    except Exception as e:
        return None, str(e)


# ---------------------------------------------------------------------------
# Outbound: fetch remote message index for a channel
# ---------------------------------------------------------------------------

async def get_peer_users(peer: dict) -> list[dict]:
    """Fetch public user list from a peer for member management."""
    ssl_ctx = ssl_manager.peer_ssl_context(peer.get("ssl_fingerprint"))
    try:
        async with _get_session().ws_connect(
                peer["address"] + "/peer", ssl=ssl_ctx,
                timeout=aiohttp.ClientWSTimeout(ws_close=5),
            ) as ws:
                await ws.send_str(_dumps({"type": "read_users"}))
                raw  = await asyncio.wait_for(ws.receive_str(), timeout=5)
                resp = json.loads(raw)
                users = resp.get("users", [])
                # Tag each user with which peer they belong to
                for u in users:
                    u["peer_id"]   = str(peer["id"])
                    u["peer_name"] = peer.get("name") or peer["address"]
                return users
    except Exception as e:
        log.warning("Could not fetch users from %s: %s", peer["address"], e)
        return []


async def get_peer_channels(peer: dict) -> list[dict]:
    """Fetch public channels from a peer server."""
    ssl_ctx = ssl_manager.peer_ssl_context(peer.get("ssl_fingerprint"))
    try:
        async with _get_session().ws_connect(
                peer["address"] + "/peer", ssl=ssl_ctx,
                timeout=aiohttp.ClientWSTimeout(ws_close=5),
            ) as ws:
                our_address = await db.setting_get("peer_address") or _detect_outbound_address()
                await ws.send_str(_dumps({"type": "read_channels", "from": our_address}))
                raw  = await asyncio.wait_for(ws.receive_str(), timeout=5)
                resp = json.loads(raw)
                # Update peer name if it changed
                new_name = resp.get("name") or None
                if new_name and new_name != peer.get("name"):
                    await db.peer_upsert(peer["address"], new_name, peer.get("ssl_fingerprint"))
                return resp.get("channels", [])
    except Exception as e:
        log.warning("Could not fetch channels from %s: %s %r", peer["address"], type(e).__name__, e)
        return []


async def get_remote_messages(peer: dict, channel_id: uuid.UUID, requesting_user_id: uuid.UUID) -> list[dict]:
    """
    Ask a peer for the message index of a channel.
    Returns list of message index dicts (no content — content is fetched
    lazily by the client when it needs to display a message).
    """
    ssl_ctx     = ssl_manager.peer_ssl_context(peer.get("ssl_fingerprint"))
    our_address = await db.setting_get("peer_address") or _detect_outbound_address()

    try:
        async with _get_session().ws_connect(
                peer["address"] + "/peer",
                ssl=ssl_ctx,
                timeout=aiohttp.ClientWSTimeout(ws_close=config.PEER_CONNECT_TIMEOUT),
            ) as ws:
                # Fetch the message index
                await ws.send_str(_dumps({
                    "type":       "read_channel",
                    "channel_id": channel_id,
                    "from":       our_address,
                }))
                raw  = await asyncio.wait_for(ws.receive_str(), timeout=15)
                resp = json.loads(raw)
                if not resp.get("ok"):
                    log.warning("read_channel rejected by %s: %s", peer["address"], resp.get("reason"))
                    return []

                # Subscribe for live updates so new messages are pushed to us
                await ws.send_str(_dumps({
                    "type":       "subscribe",
                    "channel_id": channel_id,
                    "from":       our_address,
                }))
                await asyncio.wait_for(ws.receive_str(), timeout=5)

                messages = resp.get("messages", [])
                base_url = peer["address"].replace("wss://", "https://").replace("ws://", "http://")
                for m in messages:
                    img = m.get("image")
                    if img and not str(img).startswith(("http", "/")):
                        m["image"] = f"/proxy_img?url={base_url}/img/{img}"
                return messages
    except Exception as e:
        log.warning("Failed to fetch remote channel %s from %s: %s", channel_id, peer["address"], e)
        return []


# ---------------------------------------------------------------------------
# Outbound: notify peers about a new local message
# ---------------------------------------------------------------------------

async def notify_peers_of_message(
    channel_id: uuid.UUID,
    msg_id: uuid.UUID,
    sender_user_id: uuid.UUID,
    created: datetime,
    sender_name: str = "",
    sender_avatar: str | None = None,
):
    """
    Notify all peers subscribed to a channel that a new message exists.
    Peers will call back to fetch content when a user opens the message.
    """
    subscribers = list(_peer_subscriptions.get(channel_id, []))
    if not subscribers:
        return

    our_address = await db.setting_get("peer_address") or _detect_outbound_address()
    base_url    = our_address.replace("wss://", "https://").replace("ws://", "http://")
    avatar_url  = f"{base_url}/img/{sender_avatar}" if sender_avatar else None
    payload = _dumps({
        "type":           "new_message",
        "channel_id":     channel_id,
        "message_id":     msg_id,
        "sender_user_id": sender_user_id,
        "sender_address": our_address,
        "sender_name":    sender_name,
        "sender_avatar":  avatar_url,
        "created":        created,
    })

    peers = {p["address"]: p for p in await db.peers_all()}

    async def _notify(address: str):
        peer = peers.get(address)
        if not peer:
            _remove_peer_subscriptions(address)
            return
        ssl_ctx = ssl_manager.peer_ssl_context(peer.get("ssl_fingerprint"))
        try:
            async with _get_session().ws_connect(
                    address + "/peer", ssl=ssl_ctx,
                    timeout=aiohttp.ClientWSTimeout(ws_close=config.PEER_CONNECT_TIMEOUT),
                ) as ws:
                    await ws.send_str(payload)
        except Exception as e:
            log.warning("Failed to notify peer %s: %s", address, e)
            _remove_peer_subscriptions(address)

    await asyncio.gather(*[_notify(addr) for addr in subscribers], return_exceptions=True)


# ---------------------------------------------------------------------------
# Outbound: fetch single message content from a peer
# ---------------------------------------------------------------------------

async def send_message_to_peer(
    peer: dict,
    channel_id: uuid.UUID,
    msg_id: uuid.UUID,
    sender_user_id: uuid.UUID,
    sender_address: str,
    created: datetime,
    sender_name: str = "",
    sender_avatar: str | None = None,
    parent_id: uuid.UUID | None = None,
) -> bool:
    """Send a new message notification to the channel's server."""
    ssl_ctx = ssl_manager.peer_ssl_context(peer.get("ssl_fingerprint"))
    try:
        async with _get_session().ws_connect(
                peer["address"] + "/peer", ssl=ssl_ctx,
                timeout=aiohttp.ClientWSTimeout(ws_close=config.PEER_CONNECT_TIMEOUT),
            ) as ws:
                await ws.send_str(_dumps({
                    "type":           "new_message",
                    "message_id":     msg_id,
                    "channel_id":     channel_id,
                    "sender_user_id": sender_user_id,
                    "sender_address": sender_address,
                    "sender_name":    sender_name,
                    "sender_avatar":  sender_avatar,
                    "parent_id":      parent_id,
                    "created":        created,
                }))
                raw  = await asyncio.wait_for(ws.receive_str(), timeout=10)
                resp = json.loads(raw)
                return resp.get("ok", False)
    except Exception as e:
        log.warning("Failed to send message to peer %s: %s %r", peer["address"], type(e).__name__, e)
        return False


async def fetch_message_content(peer: dict, msg_id: uuid.UUID) -> dict | None:
    """
    Fetch the actual text/image of a remote message.
    Returns None if the peer is unreachable (client shows 'Unavailable').
    """
    ssl_ctx = ssl_manager.peer_ssl_context(peer.get("ssl_fingerprint"))
    try:
        async with _get_session().ws_connect(
                peer["address"] + "/peer",
                ssl=ssl_ctx,
                timeout=aiohttp.ClientWSTimeout(ws_close=config.PEER_CONNECT_TIMEOUT),
            ) as ws:
                await ws.send_str(_dumps({"type": "get_message", "id": msg_id}))
                raw  = await asyncio.wait_for(ws.receive_str(), timeout=10)
                resp = json.loads(raw)
                if resp.get("ok"):
                    return resp.get("message")
                return None
    except Exception as e:
        log.warning("Failed to fetch message %s from %s: %s", msg_id, peer["address"], e)
        return None


# ---------------------------------------------------------------------------
# Inbound: /peer WebSocket handler
# ---------------------------------------------------------------------------

async def peer_ws_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    peer_address: str | None = None

    try:
        async for msg in ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                break

            try:
                data = json.loads(msg.data)
            except json.JSONDecodeError:
                await ws.send_str(_dumps(_err("parse", "Invalid JSON")))
                continue

            msg_type = data.get("type", "")

            # --- peer_details: exchange identities ---
            if msg_type == "peer_details":
                from_address = data.get("from_address", "")
                from_name    = data.get("from_name", "")
                if from_address:
                    # Register the caller as a peer (bidirectional)
                    await db.peer_upsert(from_address, from_name or None, None)

                name    = await db.setting_get("peer_name") or ""
                address = await db.setting_get("peer_address") or _detect_outbound_address()
                await ws.send_str(_dumps(_ok("peer_details", name=name, address=address)))

            # --- read_channels: return channels visible to the requesting peer ---
            elif msg_type == "read_channels":
                from_address = data.get("from", "")
                peer_record  = await db.peer_by_address(from_address) if from_address else None
                if peer_record:
                    channels = await db.channels_visible_to_peer(peer_record["id"])
                else:
                    channels = await db.channels_all_public()
                peer_name    = await db.setting_get("peer_name") or ""
                peer_address = await db.setting_get("peer_address") or _detect_outbound_address()
                await ws.send_str(_dumps(_ok("read_channels",
                    channels=channels, name=peer_name, address=peer_address)))

            # --- read_users: return public user list for member management ---
            elif msg_type == "read_users":
                users = await db.users_all()
                await ws.send_str(_dumps(_ok("read_users", users=users)))

            # --- read_channel: return local message index for a channel ---
            elif msg_type == "read_channel":
                cid = _str_uuid(data.get("channel_id"))
                if not cid:
                    await ws.send_str(_dumps(_err("read_channel", "Missing channel_id")))
                    continue

                channel = await db.channel_by_id(cid)
                if not channel:
                    await ws.send_str(_dumps(_err("read_channel", "Channel not found")))
                    continue

                if not channel["public"]:
                    from_address = data.get("from", "")
                    peer_record  = await db.peer_by_address(from_address) if from_address else None
                    if not peer_record or not await db.peer_has_member_in_channel(cid, peer_record["id"]):
                        await ws.send_str(_dumps(_err("read_channel", "Not authorised")))
                        continue

                messages  = await db.messages_for_channel(cid)
                base_url  = (await db.setting_get("peer_address") or _detect_outbound_address()) \
                            .replace("wss://", "https://").replace("ws://", "http://")
                for m in messages:
                    av = m.get("sender_avatar")
                    if av and not str(av).startswith("http"):
                        m["sender_avatar"] = f"{base_url}/img/{av}"
                await ws.send_str(_dumps(_ok("read_channel", messages=messages)))

            # --- get_message: return content of a single message ---
            elif msg_type == "get_message":
                mid = _str_uuid(data.get("id"))
                if not mid:
                    await ws.send_str(_dumps(_err("get_message", "Missing id")))
                    continue

                # First check message_content directly — covers messages we sent
                # to remote channels (no local message_index entry for those).
                content = await db.message_content_local(mid)
                if content is not None:
                    await ws.send_str(_dumps(_ok("get_message", message=content)))
                    continue

                # Fall back to full message lookup for local channel messages
                message = await db.message_by_id(mid)
                if not message or message.get("sender_peer_id") is not None:
                    await ws.send_str(_dumps(_err("get_message", "Not found")))
                    continue

                await ws.send_str(_dumps(_ok("get_message", message=message)))

            # --- subscribe: peer wants live notifications for a channel ---
            elif msg_type == "subscribe":
                cid          = _str_uuid(data.get("channel_id"))
                from_address = data.get("from", "")
                if not cid or not from_address:
                    await ws.send_str(_dumps(_err("subscribe", "Missing channel_id or from")))
                    continue

                channel = await db.channel_by_id(cid)
                if channel and not channel["public"]:
                    peer_record = await db.peer_by_address(from_address)
                    if not peer_record or not await db.peer_has_member_in_channel(cid, peer_record["id"]):
                        await ws.send_str(_dumps(_err("subscribe", "Not authorised")))
                        continue

                peer_address = from_address
                _add_peer_subscription(cid, from_address)
                await ws.send_str(_dumps(_ok("subscribe", channel_id=cid)))

            # --- new_message: a peer is telling us about a new message ---
            elif msg_type == "new_message":
                mid            = _str_uuid(data.get("message_id"))
                cid            = _str_uuid(data.get("channel_id"))
                sender_user_id = _str_uuid(data.get("sender_user_id"))
                sender_address = data.get("sender_address", "")
                created_raw    = data.get("created")

                if not all([mid, cid, sender_user_id]):
                    await ws.send_str(_dumps(_err("new_message", "Missing required fields")))
                    continue

                try:
                    created = datetime.fromisoformat(created_raw)
                except (ValueError, TypeError):
                    created = datetime.now(timezone.utc)

                sender_name   = data.get("sender_name", "")
                sender_avatar = data.get("sender_avatar")
                parent_id     = _str_uuid(data.get("parent_id"))

                channel = await db.channel_by_id(cid)

                if channel:
                    # Channel is hosted here — store the message index with access checks
                    peer_record = None
                    if sender_address:
                        peer_record = await db.peer_by_address(sender_address)
                        if not peer_record:
                            if not channel["public"]:
                                await ws.send_str(_dumps(_err("new_message", "Not authorised")))
                                continue
                            peer_record = await db.peer_upsert(sender_address, None, None)

                    peer_id_val = peer_record["id"] if peer_record else None

                    if not channel["public"]:
                        if not peer_id_val or not await db.is_remote_member(cid, sender_user_id, peer_id_val):
                            await ws.send_str(_dumps(_err("new_message", "Not authorised")))
                            continue

                    if peer_id_val and sender_name:
                        await db.user_cache_upsert(sender_user_id, peer_id_val, sender_name, sender_avatar)

                    await db.message_index_add(
                        mid, cid, sender_user_id, peer_id_val, parent_id, created
                    )
                else:
                    # Channel lives on another server — we're just a subscriber relay
                    peer_record  = await db.peer_by_address(sender_address) if sender_address else None
                    peer_id_val  = peer_record["id"] if peer_record else None

                await ws.send_str(_dumps(_ok("new_message", message_id=mid)))

                # Push to any local clients watching this channel
                from handlers.ws import broadcast_to_channel
                await broadcast_to_channel(cid, {
                    "type":            "message",
                    "ok":              True,
                    "message": {
                        "id":              str(mid),
                        "channel_id":      str(cid),
                        "sender_user_id":  str(sender_user_id),
                        "sender_peer_id":  str(peer_id_val) if peer_id_val else None,
                        "sender_name":     sender_name,
                        "sender_avatar":   sender_avatar,
                        "peer_address":    sender_address,
                        "peer_name":       peer_record.get("name", sender_address) if peer_record else sender_address,
                        "parent_id":       str(parent_id) if parent_id else None,
                        "text":            None,
                        "image":           None,
                        "created":         created.isoformat(),
                        "remote":          True,
                    }
                })

            else:
                await ws.send_str(_dumps(_err(msg_type, f"Unknown type: {msg_type}")))

    except Exception:
        log.exception("Peer WS error from %s", peer_address)

    return ws
