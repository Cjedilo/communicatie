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
import secrets
import time as _time
import uuid
from contextlib import asynccontextmanager
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
    return f"wss://{ip}:{config.PORT}{config.BASE_PATH}"


async def _channel_owner_info(channel: dict) -> dict | None:
    """Describe a local channel's creator to a remote peer, so a denied
    visitor/sender can be pointed at who to ask for access."""
    if not channel.get("created_by"):
        return None
    owner = await db.user_by_id(channel["created_by"])
    if not owner:
        return None
    base_url = (await db.setting_get("peer_address") or _detect_outbound_address()) \
                .replace("wss://", "https://").replace("ws://", "http://")
    return {
        "id":     owner["id"],
        "name":   owner.get("display_name") or owner["name"],
        "avatar": (f"{base_url}/img/{owner['avatar']}" if owner.get("avatar") else None),
    }


# ---------------------------------------------------------------------------
# Shared HTTP/WS client session (reuses connections)
# ---------------------------------------------------------------------------

_client_session: aiohttp.ClientSession | None = None


def _get_session() -> aiohttp.ClientSession:
    global _client_session
    if _client_session is None or _client_session.closed:
        _client_session = aiohttp.ClientSession()
    return _client_session


async def close_session(app=None):
    global _client_session
    if _client_session and not _client_session.closed:
        await _client_session.close()
        _client_session = None


# ---------------------------------------------------------------------------
# Subscriptions: channel_id → set of peer addresses watching it
# ---------------------------------------------------------------------------

_peer_subscriptions:   dict[uuid.UUID, set[str]] = {}
_stream_subscriptions: set[str] = set()          # peers subscribed to all messages
_connected_peers:      set[str] = set()           # addresses with active inbound /peer WS


def get_connected_peers() -> frozenset[str]:
    return frozenset(_connected_peers)


def _add_peer_subscription(channel_id: uuid.UUID, peer_address: str):
    _peer_subscriptions.setdefault(channel_id, set()).add(peer_address)


def _remove_peer_subscriptions(peer_address: str):
    for subs in _peer_subscriptions.values():
        subs.discard(peer_address)
    _stream_subscriptions.discard(peer_address)


# ---------------------------------------------------------------------------
# Persistent outbound session pool — one WS connection per peer, reused
# ---------------------------------------------------------------------------

class _PeerSession:
    """
    Persistent outbound WebSocket connection to a single peer.

    Push messages (new_message, stream_message) always arrive INBOUND on our
    own /peer endpoint — never on this outbound connection — so there is no
    response/push ambiguity: every message we receive here is a direct reply
    to our last request.
    """

    def __init__(self, peer: dict):
        self.peer         = peer
        self._ws:         aiohttp.ClientWebSocketResponse | None = None
        self._lock        = asyncio.Lock()
        self._failed      = False
        self._awaiting    = False   # pending approval on the remote side
        self._fail_reason: str = ""
        self._last_used   = 0.0

    @property
    def status(self) -> str:
        if self._awaiting:
            return "awaiting"
        if self._failed:
            return "failed"
        if self._ws is None or self._ws.closed:
            return "offline"   # was connected before, now cleaned up or closed
        elapsed = _time.monotonic() - self._last_used
        return "connected" if elapsed < 60 else "idle"

    async def _open(self):
        ssl_ctx = ssl_manager.peer_ssl_context(self.peer.get("ssl_fingerprint"))
        try:
            self._ws = await _get_session().ws_connect(
                self.peer["address"] + "/peer",
                ssl=ssl_ctx,
                heartbeat=30,
                timeout=aiohttp.ClientWSTimeout(ws_close=config.PEER_CONNECT_TIMEOUT),
            )
            self._awaiting    = False
            self._failed      = False
            self._fail_reason = ""
        except Exception as e:
            self._awaiting    = False
            self._failed      = True
            self._fail_reason = str(e)
            self._ws          = None
            raise

    async def _ensure(self):
        if not self._ws or self._ws.closed:
            await self._open()

    def _with_token(self, msg: dict) -> dict:
        """Attach our shared secret for this peer so they can authenticate the request."""
        token = self.peer.get("auth_token")
        if token and "auth_token" not in msg:
            return {**msg, "auth_token": token}
        return msg

    async def _ensure_token(self):
        """Lazily obtain the shared secret via a peer_details handshake if we don't
        have one yet (e.g. right after the remote side approved us). Runs at most
        one extra round-trip and only while the token is still missing."""
        if self.peer.get("auth_token"):
            return
        # Another code path may have already stored it — refresh from DB first.
        refreshed = await db.peer_by_id(self.peer["id"])
        if refreshed and refreshed.get("auth_token"):
            self.peer = refreshed
            return
        our_name    = await db.setting_get("peer_name") or ""
        our_address = await db.setting_get("peer_address") or _detect_outbound_address()
        await self._ws.send_str(_dumps({
            "type": "peer_details", "from_address": our_address, "from_name": our_name,
        }))
        raw     = await asyncio.wait_for(self._ws.receive_str(), timeout=10)
        details = json.loads(raw)
        token   = details.get("auth_token")
        if token:
            await db.peer_set_auth_token(self.peer["id"], token)
            refreshed = await db.peer_by_id(self.peer["id"])
            if refreshed:
                self.peer = refreshed

    async def request(self, msg: dict, timeout: int = 15) -> dict:
        """Send one message and return the direct response."""
        async with self._lock:
            await self._ensure()
            await self._ensure_token()
            try:
                await self._ws.send_str(_dumps(self._with_token(msg)))
                self._last_used = _time.monotonic()
                raw = await asyncio.wait_for(self._ws.receive_str(), timeout=timeout)
            except Exception:
                # A timeout/error after sending can leave the unread reply on the
                # socket; drop it so the next request reconnects instead of reading
                # a stale response.
                await self.close()
                raise
            return json.loads(raw)

    @asynccontextmanager
    async def pipeline(self):
        """
        Context manager for pipelined operations (multiple send/recv on one
        lock acquisition).  Yields the raw aiohttp.ClientWebSocketResponse.
        """
        async with self._lock:
            await self._ensure()
            await self._ensure_token()
            try:
                yield self._ws
                self._last_used = _time.monotonic()
            except Exception:
                # A mid-pipeline failure can leave unconsumed frames on the wire;
                # drop the socket so the next use reconnects cleanly.
                await self.close()
                raise

    async def close(self):
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws    = None
        self._failed = False


_sessions: dict[str, _PeerSession] = {}   # peer address → session


def _session(peer: dict) -> _PeerSession:
    addr = peer["address"]
    if addr not in _sessions:
        _sessions[addr] = _PeerSession(peer)
    else:
        _sessions[addr].peer = peer   # refresh peer info (name, fp, etc.)
    return _sessions[addr]


def get_peer_statuses() -> dict[str, dict]:
    """Return {address: {status, reason}} for all known sessions."""
    return {
        addr: {"status": s.status, "reason": s._fail_reason}
        for addr, s in _sessions.items()
    }


async def cleanup_idle_sessions():
    """Close sessions idle for more than 10 minutes."""
    cutoff = _time.monotonic() - 600
    for addr, session in list(_sessions.items()):
        if session._last_used < cutoff and session._ws and not session._ws.closed:
            log.debug("Closing idle peer session: %s", addr)
            await session.close()


async def drop_peer_session(address: str):
    """Close and discard the session for a peer (call on peer deletion)."""
    session = _sessions.pop(address, None)
    if session:
        await session.close()


# ---------------------------------------------------------------------------
# Outbound: connect to a peer
# ---------------------------------------------------------------------------

async def _resolve_succession(peer_address: str, known_fp: str, actual_fp: str) -> bool:
    """
    Fetch /succession from a peer and walk the chain from known_fp to actual_fp.
    Each step is verified with the old cert's public key (self-contained record).
    Works even if the peer was offline during the rotation — they serve the record
    whenever they're back up.
    Returns True if a valid chain is found, False otherwise.
    """
    import ssl as _ssl
    http_url = peer_address.replace("wss://", "https://").replace("ws://", "http://")
    ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode    = _ssl.CERT_NONE
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{http_url}/succession", ssl=ctx,
                             timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return False
                data = await resp.json()
    except Exception as e:
        log.warning("Could not fetch succession from %s: %s", peer_address, e)
        return False

    by_old = {r["old_fingerprint"]: r for r in data.get("records", [])}
    current = known_fp
    for _ in range(20):
        record = by_old.get(current)
        if not record:
            break
        if not ssl_manager.verify_succession_step(record):
            log.warning("Invalid succession signature in chain from %s", peer_address)
            return False
        current = record["new_fingerprint"]
        if current == actual_fp:
            return True

    return False


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

                if not details.get("ok", True):
                    reason = details.get("reason", "Connection rejected")
                    if reason == "Pending approval":
                        # Store the peer so it appears in the list with awaiting status.
                        # The rejection response carries no name/address/fingerprint,
                        # so use the address we dialled and leave the rest unset.
                        peer = await db.peer_upsert(address, details.get("name"), None)
                        sess = _session(peer)
                        sess._awaiting    = True
                        sess._failed      = False
                        sess._fail_reason = reason
                        return peer, reason
                    return None, reason

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
                            resolved = await _resolve_succession(address, fingerprint, fp)
                            if not resolved:
                                return None, "Certificate fingerprint mismatch — possible MITM"
                            log.info("Succession accepted for %s (%s → %s)",
                                     address, fingerprint[:12], fp[:12])

                peer = await db.peer_upsert(peer_address, peer_name, fp)
                await db.peer_update_last_seen(peer_address)

                # Store the shared secret the peer issued us (only sent once, on
                # first approved handshake). We present it on every later request.
                token = details.get("auth_token")
                if token:
                    await db.peer_set_auth_token(peer["id"], token)
                    peer = await db.peer_by_address(peer_address)

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
    our_address = await db.setting_get("peer_address") or _detect_outbound_address()
    try:
        resp     = await _session(peer).request(
            {"type": "read_users", "from": our_address}, timeout=5)
        users    = resp.get("users", [])
        base_url = peer["address"].replace("wss://", "https://").replace("ws://", "http://")
        for u in users:
            u["peer_id"]   = str(peer["id"])
            u["peer_name"] = peer.get("name") or peer["address"]
            av = u.get("avatar")
            if av and not str(av).startswith("http"):
                u["avatar"] = f"{config.BASE_PATH}/proxy_img?url={base_url}/img/{av}"
        return users
    except Exception as e:
        log.warning("Could not fetch users from %s: %s", peer["address"], e)
        return []


async def get_peer_channels(peer: dict, user_id=None) -> list[dict]:
    """Fetch channels visible to a specific user from a peer server."""
    try:
        our_address = await db.setting_get("peer_address") or _detect_outbound_address()
        req = {"type": "read_channels", "from": our_address}
        if user_id:
            req["user_id"] = str(user_id)
        resp = await _session(peer).request(req, timeout=5)
        new_name = resp.get("name") or None
        if new_name and new_name != peer.get("name"):
            await db.peer_upsert(peer["address"], new_name, peer.get("ssl_fingerprint"))
        return resp.get("channels", [])
    except Exception as e:
        log.warning("Could not fetch channels from %s: %s %r", peer["address"], type(e).__name__, e)
        return []


async def get_peer_stream_messages(peer: dict, max_channels: int = 5, limit_per_channel: int = 10, channel_cursors: dict | None = None, subscribed_channels: set | None = None) -> list[dict]:
    """Fetch recent messages from a peer, pipelined on the persistent session."""
    our_address = await db.setting_get("peer_address") or _detect_outbound_address()
    base_url    = peer["address"].replace("wss://", "https://").replace("ws://", "http://")

    sess = _session(peer)
    try:
        async with sess.pipeline() as ws:
            tok = sess.peer.get("auth_token")   # refreshed by pipeline()'s token handshake
            # Read channels
            await ws.send_str(_dumps({"type": "read_channels", "from": our_address, "auth_token": tok}))
            channels_resp = json.loads(await asyncio.wait_for(ws.receive_str(), timeout=5))
            channels      = channels_resp.get("channels", [])

            new_name = channels_resp.get("name") or None
            if new_name and new_name != peer.get("name"):
                await db.peer_upsert(peer["address"], new_name, peer.get("ssl_fingerprint"))
            peer_name = new_name or peer.get("name") or ""

            channels.sort(key=lambda c: c.get("last_activity") or "", reverse=True)
            def _ch_visible(c):
                if not c.get("id"):
                    return False
                if c.get("public"):
                    key = f"{c['id']}|{str(peer['id'])}"
                    return subscribed_channels is not None and key in subscribed_channels
                return True
            channels = [c for c in channels if _ch_visible(c)][:max_channels]

            # Pipeline: subscribe_stream + all read_channel in one RTT
            await ws.send_str(_dumps({"type": "subscribe_stream", "from": our_address, "auth_token": tok}))
            for ch in channels:
                req = {"type": "read_channel", "channel_id": ch["id"], "from": our_address, "auth_token": tok}
                if channel_cursors:
                    ch_key = f"{ch['id']}|{str(peer['id'])}"
                    if ch_key in channel_cursors:
                        req["before"] = channel_cursors[ch_key]
                        req["limit"]  = limit_per_channel
                await ws.send_str(_dumps(req))

            await asyncio.wait_for(ws.receive_str(), timeout=5)   # subscribe_stream ack

            all_messages = []
            for ch in channels:
                resp = json.loads(await asyncio.wait_for(ws.receive_str(), timeout=5))
                if not resp.get("ok"):
                    continue
                messages = resp.get("messages", [])[-limit_per_channel:]
                for m in messages:
                    img = m.get("image")
                    if img and not str(img).startswith(("http", "/")) and img != "📷":
                        m["image"] = f"{config.BASE_PATH}/proxy_img?url={base_url}/img/{img}"
                    av = m.get("sender_avatar")
                    if av:
                        if not str(av).startswith(("http", "/")):
                            av = f"{base_url}/img/{av}"
                        m["sender_avatar"] = f"{config.BASE_PATH}/proxy_img?url={av}"
                    m["channel_name"]   = ch.get("name", "")
                    m["channel_public"] = ch.get("public", True)
                    m["channel_icon"]   = ch.get("icon")
                    m["peer_name"]      = peer_name
                    m["peer_id"]        = str(peer["id"])
                all_messages.extend(messages)

            return all_messages
    except Exception as e:
        log.warning("Could not fetch stream messages from %s: %s %r", peer["address"], type(e).__name__, e)
        return []


async def get_remote_messages(peer: dict, channel_id: uuid.UUID, requesting_user_id: uuid.UUID) -> tuple[dict | None, list[dict]]:
    """Ask a peer for the message index of a channel, then subscribe for live updates."""
    our_address = await db.setting_get("peer_address") or _detect_outbound_address()
    base_url    = peer["address"].replace("wss://", "https://").replace("ws://", "http://")
    sess = _session(peer)
    try:
        async with sess.pipeline() as ws:
            tok = sess.peer.get("auth_token")   # refreshed by pipeline()'s token handshake
            await ws.send_str(_dumps({
                "type": "read_channel", "channel_id": channel_id, "from": our_address,
                "user_id": str(requesting_user_id), "auth_token": tok,
            }))
            resp = json.loads(await asyncio.wait_for(ws.receive_str(), timeout=15))
            if not resp.get("ok"):
                log.warning("read_channel rejected by %s: %s", peer["address"], resp.get("reason"))
                if resp.get("channel"):
                    denied = dict(resp["channel"])
                    denied["access_denied"] = True
                    denied["owner"] = resp.get("owner")
                    return denied, []
                return None, []

            await ws.send_str(_dumps({
                "type": "subscribe", "channel_id": channel_id, "from": our_address,
                "auth_token": tok,
            }))
            await asyncio.wait_for(ws.receive_str(), timeout=5)

            channel  = resp.get("channel")
            messages = resp.get("messages", [])
            for m in messages:
                img = m.get("image")
                if img and not str(img).startswith(("http", "/")) and img != "📷":
                    m["image"] = f"{config.BASE_PATH}/proxy_img?url={base_url}/img/{img}"
                av = m.get("sender_avatar")
                if av:
                    if not str(av).startswith(("http", "/")):
                        av = f"{base_url}/img/{av}"
                    m["sender_avatar"] = f"{config.BASE_PATH}/proxy_img?url={av}"
            return channel, messages
    except Exception as e:
        log.warning("Failed to fetch remote channel %s from %s: %s", channel_id, peer["address"], e)
        return None, []


# ---------------------------------------------------------------------------
# Outbound: notify peers about a new local message
# ---------------------------------------------------------------------------

async def _subscriber_recipients(channel_id: uuid.UUID, channel: dict | None,
                                  peers: dict[str, dict]) -> set[str]:
    """Per-channel subscribers + stream subscribers with access — the
    fan-out audience for any push about a message in this channel."""
    recipients = set(_peer_subscriptions.get(channel_id, []))
    for addr in list(_stream_subscriptions):
        if addr in recipients:
            continue
        if channel and not channel["public"]:
            peer = peers.get(addr)
            if not peer:
                continue
            if not await db.peer_has_member_in_channel(channel_id, peer["id"]):
                continue
        recipients.add(addr)
    return recipients


async def _fanout_to_peers(payload: str, recipients: set[str], peers: dict[str, dict]):
    if not recipients:
        return

    async def _notify(address: str):
        peer = peers.get(address)
        if not peer:
            _remove_peer_subscriptions(address)
            return
        try:
            await _session(peer).request(json.loads(payload), timeout=config.PEER_CONNECT_TIMEOUT)
        except Exception as e:
            log.warning("Failed to notify peer %s: %s", address, e)
            _remove_peer_subscriptions(address)

    await asyncio.gather(*[_notify(addr) for addr in recipients], return_exceptions=True)


async def notify_peers_of_message(
    channel_id: uuid.UUID,
    msg_id: uuid.UUID,
    sender_user_id: uuid.UUID,
    created: datetime,
    sender_name: str = "",
    sender_avatar: str | None = None,
    parent_id: uuid.UUID | None = None,
):
    """
    Notify all peers subscribed to a channel that a new message exists.
    Includes message text so subscribers can show previews without a round-trip.
    """
    our_address = await db.setting_get("peer_address") or _detect_outbound_address()
    base_url    = our_address.replace("wss://", "https://").replace("ws://", "http://")
    avatar_url  = f"{base_url}/img/{sender_avatar}" if sender_avatar else None
    channel     = await db.channel_by_id(channel_id)
    content     = await db.message_content_local(msg_id)
    msg_text    = content.get("text") if content else None
    has_image   = bool(content.get("image")) if content else False
    payload = _dumps({
        "type":           "new_message",
        "channel_id":     channel_id,
        "message_id":     msg_id,
        "sender_user_id": sender_user_id,
        "sender_address": our_address,
        "sender_name":    sender_name,
        "sender_avatar":  avatar_url,
        "channel_name":   channel["name"] if channel else "",
        "channel_public": channel["public"] if channel else True,
        "text":           msg_text,
        "has_image":      has_image,
        "created":        created,
        "parent_id":      parent_id,
    })

    peers      = {p["address"]: p for p in await db.peers_all()}
    recipients = await _subscriber_recipients(channel_id, channel, peers)
    await _fanout_to_peers(payload, recipients, peers)


async def notify_peers_of_edit(
    channel_id: uuid.UUID,
    msg_id: uuid.UUID,
    text: str | None,
    image: str | None,
    edited_at: datetime,
    exclude_peer: str | None = None,
):
    """Notify all peers subscribed to a channel that one of its messages was edited."""
    channel = await db.channel_by_id(channel_id)
    payload = _dumps({
        "type":       "edit_message",
        "channel_id": channel_id,
        "message_id": msg_id,
        "text":       text,
        "has_image":  bool(image),
        "edited_at":  edited_at,
    })

    peers      = {p["address"]: p for p in await db.peers_all()}
    recipients = await _subscriber_recipients(channel_id, channel, peers)
    recipients.discard(exclude_peer)
    await _fanout_to_peers(payload, recipients, peers)


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
    text: str | None = None,
    has_image: bool = False,
) -> tuple[bool, dict]:
    """Send a new message notification to the channel's server.
    Returns (ok, info) — on rejection info carries the server's reason and,
    for a membership denial, the channel/owner so the sender can ask for access."""
    try:
        resp = await _session(peer).request({
            "type":           "new_message",
            "message_id":     msg_id,
            "channel_id":     channel_id,
            "sender_user_id": sender_user_id,
            "sender_address": sender_address,
            "sender_name":    sender_name,
            "sender_avatar":  sender_avatar,
            "parent_id":      parent_id,
            "created":        created,
            "text":           text,
            "has_image":      has_image,
        }, timeout=10)
        if resp.get("ok"):
            return True, {}
        return False, {
            "reason":  resp.get("reason"),
            "channel": resp.get("channel"),
            "owner":   resp.get("owner"),
        }
    except Exception as e:
        log.warning("Failed to send message to peer %s: %s %r", peer["address"], type(e).__name__, e)
        return False, {"reason": "Could not reach that server"}


async def get_peer_user_messages(peer: dict, user_id: uuid.UUID) -> list[dict]:
    """Fetch recent messages by a user from a peer server."""
    our_address = await db.setting_get("peer_address") or _detect_outbound_address()
    try:
        resp = await _session(peer).request({
            "type": "read_user_messages",
            "user_id": user_id,
            "from":    our_address,
        }, timeout=10)
        return resp.get("messages", [])
    except Exception as e:
        log.warning("get_peer_user_messages from %s: %s", peer["address"], e)
        return []


async def fetch_message_content(peer: dict, msg_id: uuid.UUID) -> dict | None:
    """Fetch the actual text/image of a remote message."""
    our_address = await db.setting_get("peer_address") or _detect_outbound_address()
    try:
        resp = await _session(peer).request(
            {"type": "get_message", "id": msg_id, "from": our_address}, timeout=10)
        return resp.get("message") if resp.get("ok") else None
    except Exception as e:
        log.warning("Failed to fetch message %s from %s: %s", msg_id, peer["address"], e)
        return None


async def set_channel_setting_on_peer(peer: dict, channel_id: uuid.UUID,
                                       sender_user_id: uuid.UUID,
                                       setting_type: str, **kwargs) -> tuple[bool, dict]:
    """Forward a channel-setting change to the channel's hosting peer."""
    our_address = await db.setting_get("peer_address") or _detect_outbound_address()
    try:
        resp = await _session(peer).request({
            "type":           setting_type,
            "channel_id":     channel_id,
            "sender_user_id": sender_user_id,
            "from":           our_address,
            **kwargs,
        }, timeout=10)
        if resp.get("ok"):
            return True, resp
        return False, {"reason": resp.get("reason")}
    except Exception as e:
        log.warning("Failed to set %s on peer %s: %s", setting_type, peer["address"], e)
        return False, {"reason": "Could not reach that server"}


async def notify_peers_of_deletion(channel_id: uuid.UUID, msg_id: uuid.UUID,
                                    exclude_peer: str | None = None):
    """Notify all peers subscribed to a channel that a message was deleted."""
    channel = await db.channel_by_id(channel_id)
    payload = _dumps({
        "type":       "delete_message",
        "channel_id": channel_id,
        "message_id": msg_id,
    })
    peers      = {p["address"]: p for p in await db.peers_all()}
    recipients = await _subscriber_recipients(channel_id, channel, peers)
    recipients.discard(exclude_peer)
    await _fanout_to_peers(payload, recipients, peers)


async def delete_message_on_peer(peer: dict, channel_id: uuid.UUID, msg_id: uuid.UUID,
                                  sender_user_id: uuid.UUID) -> tuple[bool, dict]:
    """Ask the channel-hosting peer to delete a message we originally sent."""
    our_address = await db.setting_get("peer_address") or _detect_outbound_address()
    try:
        resp = await _session(peer).request({
            "type":           "delete_message",
            "channel_id":     channel_id,
            "message_id":     msg_id,
            "sender_user_id": sender_user_id,
            "from":           our_address,
        }, timeout=10)
        if resp.get("ok"):
            return True, {}
        return False, {"reason": resp.get("reason")}
    except Exception as e:
        log.warning("Failed to delete message on peer %s: %s %r", peer["address"], type(e).__name__, e)
        return False, {"reason": "Could not reach that server"}


async def notify_peers_of_reaction(channel_id: uuid.UUID, msg_id: uuid.UUID,
                                    reactions: list[dict], exclude_peer: str | None = None):
    """Notify all peers subscribed to a channel that a message's reactions changed."""
    channel = await db.channel_by_id(channel_id)
    payload = _dumps({
        "type":       "toggle_reaction",
        "channel_id": channel_id,
        "message_id": msg_id,
        "reactions":  reactions,
    })
    peers      = {p["address"]: p for p in await db.peers_all()}
    recipients = await _subscriber_recipients(channel_id, channel, peers)
    recipients.discard(exclude_peer)
    await _fanout_to_peers(payload, recipients, peers)


async def toggle_reaction_on_peer(peer: dict, channel_id: uuid.UUID, msg_id: uuid.UUID,
                                   sender_user_id: uuid.UUID, emoji: str) -> tuple[bool, dict]:
    """Ask the channel-hosting peer to toggle our reaction on a message."""
    our_address = await db.setting_get("peer_address") or _detect_outbound_address()
    try:
        resp = await _session(peer).request({
            "type":           "toggle_reaction",
            "channel_id":     channel_id,
            "message_id":     msg_id,
            "sender_user_id": sender_user_id,
            "emoji":          emoji,
            "from":           our_address,
        }, timeout=10)
        if resp.get("ok"):
            return True, {"reactions": resp.get("reactions", [])}
        return False, {"reason": resp.get("reason")}
    except Exception as e:
        log.warning("Failed to toggle reaction on peer %s: %s %r", peer["address"], type(e).__name__, e)
        return False, {"reason": "Could not reach that server"}


async def edit_message_on_peer(peer: dict, msg_id: uuid.UUID, sender_user_id: uuid.UUID,
                                text: str | None, image: str | None) -> tuple[bool, dict]:
    """Ask the channel-hosting peer to authorise and apply an edit to a
    message we sent into one of their channels. Returns (ok, info) — on
    success info carries edited_at and the channel's edit_mode, so the caller
    knows whether to keep a history copy of the old content locally."""
    try:
        resp = await _session(peer).request({
            "type":           "edit_message",
            "message_id":     msg_id,
            "sender_user_id": sender_user_id,
            "text":           text,
            "has_image":      bool(image),
        }, timeout=10)
        if resp.get("ok"):
            return True, {"edited_at": resp.get("edited_at"), "edit_mode": resp.get("edit_mode")}
        return False, {"reason": resp.get("reason")}
    except Exception as e:
        log.warning("Failed to edit message on peer %s: %s %r", peer["address"], type(e).__name__, e)
        return False, {"reason": "Could not reach that server"}


async def fetch_message_history(peer: dict, msg_id: uuid.UUID) -> list[dict] | None:
    """Fetch the prior-version edit history of a remote message, from
    whichever server actually owns its content."""
    our_address = await db.setting_get("peer_address") or _detect_outbound_address()
    try:
        resp = await _session(peer).request(
            {"type": "get_message_history", "id": msg_id, "from": our_address}, timeout=10)
        return resp.get("history") if resp.get("ok") else None
    except Exception as e:
        log.warning("Failed to fetch message history %s from %s: %s", msg_id, peer["address"], e)
        return None


async def _authed_peer(data: dict) -> dict | None:
    """Return the approved peer record iff the request carries that peer's valid
    shared secret. The token is matched against the peer identified by its stored
    address — so a spoofed `from`/`sender_address` without the secret is rejected."""
    from_address = data.get("from") or data.get("from_address") or data.get("sender_address") or ""
    token        = data.get("auth_token") or ""
    if not from_address or not token:
        return None
    peer = await db.peer_by_address(from_address)
    if not peer or peer.get("status", "approved") != "approved":
        return None
    stored = peer.get("auth_token")
    if not stored or not secrets.compare_digest(str(stored), str(token)):
        return None
    return peer


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
                    policy   = await db.setting_get("peer_policy") or "open"
                    existing = await db.peer_by_address(from_address)
                    status   = existing.get("status", "approved") if existing else None

                    if status == "blocked":
                        await ws.send_str(_dumps(_err("peer_details", "Blocked")))
                        break

                    if status == "pending":
                        await ws.send_str(_dumps(_err("peer_details", "Pending approval")))
                        break

                    if status is None:  # unknown peer
                        if policy == "closed":
                            await ws.send_str(_dumps(_err("peer_details", "Server is closed to new peers")))
                            break
                        if policy == "approval":
                            peer = await db.peer_upsert(from_address, from_name or None, None)
                            await db.peer_set_status(peer["id"], "pending")
                            await ws.send_str(_dumps(_err("peer_details", "Pending approval")))
                            break

                    await db.peer_upsert(from_address, from_name or None, None)
                    await db.peer_update_last_seen(from_address)
                    peer_address = from_address
                    _connected_peers.add(from_address)

                name    = await db.setting_get("peer_name") or ""
                address = await db.setting_get("peer_address") or _detect_outbound_address()
                resp    = _ok("peer_details", name=name, address=address)

                # Issue a shared secret the first time an approved peer connects.
                # Only ever returned once (when auth_token is still NULL); later
                # handshakes never re-send it, so it can't be fished out by an
                # impersonator claiming an established peer's address.
                if peer_address:
                    rec = await db.peer_by_address(peer_address)
                    if rec and rec.get("status", "approved") == "approved" and not rec.get("auth_token"):
                        token = secrets.token_urlsafe(32)
                        await db.peer_set_auth_token(rec["id"], token)
                        resp["auth_token"] = token

                await ws.send_str(_dumps(resp))

            # --- read_channels: return channels visible to the requesting peer ---
            elif msg_type == "read_channels":
                req_user_id  = _str_uuid(data.get("user_id"))
                peer_record  = await _authed_peer(data)   # None = unauthenticated → public only
                if peer_record and req_user_id:
                    channels = await db.channels_visible_to_remote_user(req_user_id, peer_record["id"])
                elif peer_record:
                    channels = await db.channels_visible_to_peer(peer_record["id"])
                else:
                    channels = await db.channels_all_public()
                # Enrich each channel with last-activity summary
                for ch in channels:
                    summary = await db.channel_last_message_summary(ch["id"])
                    if summary:
                        ch["last_activity"]    = summary["last_activity"]
                        ch["last_sender_name"] = summary["last_sender_name"]
                peer_name    = await db.setting_get("peer_name") or ""
                peer_address = await db.setting_get("peer_address") or _detect_outbound_address()
                for ch in channels:
                    ch["public"] = bool(ch["public"])
                await ws.send_str(_dumps(_ok("read_channels",
                    channels=channels, name=peer_name, address=peer_address)))

            # --- read_user_messages: return recent messages by a local user ---
            elif msg_type == "read_user_messages":
                uid         = _str_uuid(data.get("user_id"))
                peer_record = await _authed_peer(data)
                if not peer_record:
                    await ws.send_str(_dumps(_err("read_user_messages", "Not authorised")))
                    continue
                if not uid:
                    await ws.send_str(_dumps(_err("read_user_messages", "Missing user_id")))
                    continue
                msgs = await db.messages_by_user_for_peer(uid, peer_record["id"])
                await ws.send_str(_dumps(_ok("read_user_messages", messages=msgs)))

            # --- read_users: return public user list for member management ---
            elif msg_type == "read_users":
                if not await _authed_peer(data):
                    await ws.send_str(_dumps(_err("read_users", "Not authorised")))
                    continue
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
                    req_uid      = _str_uuid(data.get("user_id"))
                    peer_record  = await _authed_peer(data)
                    authorised   = False
                    if peer_record and req_uid:
                        authorised = await db.is_remote_member(cid, req_uid, peer_record["id"])
                    elif peer_record:
                        authorised = await db.peer_has_member_in_channel(cid, peer_record["id"])
                    if not authorised:
                        owner_info = await _channel_owner_info(channel)
                        await ws.send_str(_dumps(_err("read_channel", "Not authorised",
                            channel={"id": channel["id"], "name": channel["name"],
                                     "public": bool(channel["public"])},
                            owner=owner_info)))
                        continue

                before_str = data.get("before")
                ch_before  = None
                if before_str:
                    try:
                        ch_before = datetime.fromisoformat(before_str)
                    except (ValueError, TypeError):
                        pass
                ch_limit  = min(int(data.get("limit", 100)), 100)
                messages  = await db.messages_for_channel(cid, limit=ch_limit, before=ch_before)
                base_url  = (await db.setting_get("peer_address") or _detect_outbound_address()) \
                            .replace("wss://", "https://").replace("ws://", "http://")
                for m in messages:
                    av = m.get("sender_avatar")
                    if av and not str(av).startswith("http"):
                        m["sender_avatar"] = f"{base_url}/img/{av}"
                creator_name = None
                if channel.get("created_by"):
                    creator = await db.user_by_id(channel["created_by"])
                    if creator:
                        creator_name = creator.get("display_name") or creator["name"]

                # Check if the requesting remote user is a moderator of this channel.
                req_uid    = _str_uuid(data.get("user_id"))
                can_manage = bool(req_uid and await db.is_channel_moderator(cid, req_uid))

                await ws.send_str(_dumps(_ok("read_channel",
                    channel={"id": channel["id"], "name": channel["name"],
                             "public": bool(channel["public"]), "icon": channel.get("icon"),
                             "created_by_name": creator_name, "can_manage": can_manage,
                             **{k: channel.get(k) for k in (
                                 "allow_replies", "post_restricted", "restrict_replies",
                                 "allow_images", "allow_reactions", "allow_markdown",
                                 "edit_mode", "description",
                             )}},
                    messages=messages)))

            # --- get_message: return content of a single message ---
            elif msg_type == "get_message":
                peer_record = await _authed_peer(data)
                if not peer_record:
                    await ws.send_str(_dumps(_err("get_message", "Not authorised")))
                    continue
                mid = _str_uuid(data.get("id"))
                if not mid:
                    await ws.send_str(_dumps(_err("get_message", "Missing id")))
                    continue

                # Case A — message in a channel WE host (has a local index): enforce
                # channel access. Only our own-origin content is served (received
                # content is never cached, so sender_peer_id must be NULL).
                index = await db.message_by_id(mid)
                if index is not None:
                    if index.get("sender_peer_id") is not None:
                        await ws.send_str(_dumps(_err("get_message", "Not found")))
                        continue
                    channel = await db.channel_by_id(index["channel_id"])
                    if channel and not channel["public"]:
                        if not await db.peer_has_member_in_channel(channel["id"], peer_record["id"]):
                            await ws.send_str(_dumps(_err("get_message", "Not authorised")))
                            continue
                    content = await db.message_content_local(mid)
                    await ws.send_str(_dumps(_ok("get_message", message=content or index)))
                    continue

                # Case B — content we sent to a REMOTE channel (no local index). The
                # message UUID only ever reaches that channel's members, and we host
                # no metadata to check it against, so serve to the authenticated peer.
                content = await db.message_content_local(mid)
                if content is not None:
                    await ws.send_str(_dumps(_ok("get_message", message=content)))
                    continue

                await ws.send_str(_dumps(_err("get_message", "Not found")))

            # --- get_message_history: prior versions of an edited message ---
            elif msg_type == "get_message_history":
                peer_record = await _authed_peer(data)
                if not peer_record:
                    await ws.send_str(_dumps(_err("get_message_history", "Not authorised")))
                    continue
                mid = _str_uuid(data.get("id"))
                if not mid:
                    await ws.send_str(_dumps(_err("get_message_history", "Missing id")))
                    continue

                # History only ever lives alongside the content it belongs to —
                # same access shape as get_message's Case A/B above.
                index = await db.message_by_id(mid)
                if index is not None:
                    if index.get("sender_peer_id") is not None:
                        await ws.send_str(_dumps(_err("get_message_history", "Not found")))
                        continue
                    channel = await db.channel_by_id(index["channel_id"])
                    if channel and not channel["public"]:
                        if not await db.peer_has_member_in_channel(channel["id"], peer_record["id"]):
                            await ws.send_str(_dumps(_err("get_message_history", "Not authorised")))
                            continue
                    history = await db.message_edit_history_for(mid)
                    await ws.send_str(_dumps(_ok("get_message_history", history=history)))
                    continue

                content = await db.message_content_local(mid)
                if content is not None:
                    history = await db.message_edit_history_for(mid)
                    await ws.send_str(_dumps(_ok("get_message_history", history=history)))
                    continue

                await ws.send_str(_dumps(_err("get_message_history", "Not found")))

            # --- subscribe: peer wants live notifications for a channel ---
            elif msg_type in ("set_channel_icon", "set_channel_public",
                               "set_channel_allow_replies", "set_channel_post_restricted",
                               "set_channel_restrict_replies", "set_channel_allow_images",
                               "set_channel_allow_reactions", "set_channel_allow_markdown",
                               "set_channel_edit_mode", "set_channel_description"):
                peer_record = await _authed_peer(data)
                if not peer_record:
                    await ws.send_str(_dumps(_err(msg_type, "Peer authentication failed")))
                    continue
                cid     = _str_uuid(data.get("channel_id"))
                req_uid = _str_uuid(data.get("sender_user_id"))
                if not cid or not req_uid:
                    await ws.send_str(_dumps(_err(msg_type, "Missing channel_id or sender_user_id")))
                    continue
                channel = await db.channel_by_id(cid)
                if not channel:
                    await ws.send_str(_dumps(_err(msg_type, "Channel not found")))
                    continue
                # Remote moderators can manage but cannot become creator —
                # creator is always a local user.
                if not await db.is_channel_moderator(cid, req_uid):
                    await ws.send_str(_dumps(_err(msg_type, "Not authorised")))
                    continue
                # Delegate to the same local handler with a fake session object.
                from handlers.ws import _HANDLERS
                fake_session = {"user_id": req_uid}
                result = await _HANDLERS[msg_type](data, fake_session, ws)
                await ws.send_str(_dumps(result))

            elif msg_type == "subscribe":
                cid          = _str_uuid(data.get("channel_id"))
                peer_record  = await _authed_peer(data)
                if not cid or not peer_record:
                    await ws.send_str(_dumps(_err("subscribe", "Not authorised")))
                    continue

                channel = await db.channel_by_id(cid)
                if channel and not channel["public"]:
                    if not await db.peer_has_member_in_channel(cid, peer_record["id"]):
                        await ws.send_str(_dumps(_err("subscribe", "Not authorised")))
                        continue

                peer_address = peer_record["address"]
                _add_peer_subscription(cid, peer_address)
                await ws.send_str(_dumps(_ok("subscribe", channel_id=cid)))

            # --- subscribe_stream: peer wants all future messages pushed ---
            elif msg_type == "subscribe_stream":
                peer_record = await _authed_peer(data)
                if not peer_record:
                    await ws.send_str(_dumps(_err("subscribe_stream", "Not authorised")))
                    continue
                _stream_subscriptions.add(peer_record["address"])
                await ws.send_str(_dumps(_ok("subscribe_stream")))

            # --- new_message: a peer is telling us about a new message ---
            elif msg_type == "new_message":
                peer_record = await _authed_peer(data)
                if not peer_record:
                    # Server-to-server auth failure — not something the sending
                    # user can fix, keep it distinct from a per-user access denial.
                    await ws.send_str(_dumps(_err("new_message", "Peer authentication failed")))
                    continue
                sender_address = peer_record["address"]

                mid            = _str_uuid(data.get("message_id"))
                cid            = _str_uuid(data.get("channel_id"))
                sender_user_id = _str_uuid(data.get("sender_user_id"))
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
                msg_text      = data.get("text")
                has_image     = bool(data.get("has_image"))

                channel = await db.channel_by_id(cid)

                if channel:
                    # Channel is hosted here — store the message index with access checks
                    peer_id_val = peer_record["id"]

                    if not channel["public"]:
                        if not peer_id_val or not await db.is_remote_member(cid, sender_user_id, peer_id_val):
                            owner_info = await _channel_owner_info(channel)
                            await ws.send_str(_dumps(_err("new_message", "Not a member of this channel",
                                channel={"id": channel["id"], "name": channel["name"]}, owner=owner_info)))
                            continue
                    else:
                        if await db.is_banned(cid, sender_user_id, peer_id_val):
                            await ws.send_str(_dumps(_err("new_message", "You are banned from this channel")))
                            continue

                    if parent_id and not channel.get("allow_replies", True):
                        await ws.send_str(_dumps(_err("new_message", "Replies are turned off in this channel")))
                        continue

                    # post_restricted always gates top-level posts; replies are only
                    # gated too when the channel's restrict_replies sub-option is on.
                    restricted = channel.get("post_restricted") and (not parent_id or channel.get("restrict_replies", True))
                    if restricted and not await db.is_channel_moderator(cid, sender_user_id):
                        # A locally-hosted channel's creator is always a local user, so a
                        # remote sender can only ever qualify here via moderator status.
                        await ws.send_str(_dumps(_err("new_message", "Only moderators can post in this channel")))
                        continue

                    if has_image and not channel.get("allow_images", True):
                        await ws.send_str(_dumps(_err("new_message", "Images are turned off in this channel")))
                        continue

                    if peer_id_val and sender_name:
                        await db.user_cache_upsert(sender_user_id, peer_id_val, sender_name, sender_avatar)

                    await db.message_index_add(
                        mid, cid, sender_user_id, peer_id_val, parent_id, created
                    )
                    # DESIGN: content is NEVER cached for received messages.
                    # The originating server is the sole source of truth.
                    # Content is fetched on-demand when displayed; if the source
                    # is offline the viewer sees "unavailable".
                else:
                    # Channel lives on another server — we're just a subscriber relay
                    peer_id_val = peer_record["id"]

                await ws.send_str(_dumps(_ok("new_message", message_id=mid)))

                # Push to any local clients watching this channel
                from handlers.ws import broadcast_to_channel, push_to_stream, push_notification
                peer_name_val = peer_record.get("name") or sender_address
                bcast_msg = {
                    "id":              str(mid),
                    "channel_id":      str(cid),
                    "sender_user_id":  str(sender_user_id),
                    "sender_peer_id":  str(peer_id_val) if peer_id_val else None,
                    "sender_name":     sender_name,
                    "sender_avatar":   sender_avatar,
                    "peer_address":    sender_address,
                    "peer_name":       peer_name_val,
                    "parent_id":       str(parent_id) if parent_id else None,
                    "text":            msg_text,
                    "image":           "📷" if has_image and not msg_text else None,
                    "created":         created.isoformat(),
                    "remote":          True,
                }
                await broadcast_to_channel(cid, {"type": "message", "ok": True, "message": bcast_msg})
                proxy_av = (f"{config.BASE_PATH}/proxy_img?url={sender_avatar}"
                            if sender_avatar and str(sender_avatar).startswith("http")
                            else sender_avatar)
                if channel and not channel.get("stream_excluded"):
                    await push_to_stream({"type": "stream_message", "ok": True, "message": {
                        **bcast_msg,
                        "sender_avatar":  proxy_av,
                        "channel_name":   channel["name"],
                        "channel_public": channel["public"],
                    }})
                elif not channel:
                    # Relay: channel hosted elsewhere
                    relay_name   = data.get("channel_name", "")
                    relay_public = data.get("channel_public", True)
                    await push_to_stream({"type": "stream_message", "ok": True, "message": {
                        **bcast_msg,
                        "sender_avatar":  proxy_av,
                        "channel_name":   relay_name,
                        "channel_public": relay_public,
                        "peer_id":        str(peer_id_val) if peer_id_val else None,
                    }})

                await push_notification(cid, peer_id_val if not channel else None)

            # --- edit_message: a peer is editing a message — either asking us
            # (the channel host) to authorise + apply it, or just relaying an
            # edit notification onward as a subscriber ---
            elif msg_type == "edit_message":
                peer_record = await _authed_peer(data)
                if not peer_record:
                    await ws.send_str(_dumps(_err("edit_message", "Peer authentication failed")))
                    continue

                mid            = _str_uuid(data.get("message_id"))
                cid            = _str_uuid(data.get("channel_id"))
                sender_user_id = _str_uuid(data.get("sender_user_id"))
                msg_text       = data.get("text")
                has_image      = bool(data.get("has_image"))

                if not mid:
                    await ws.send_str(_dumps(_err("edit_message", "Missing message_id")))
                    continue

                channel = await db.channel_by_id(cid) if cid else None

                if channel:
                    # We host this channel's index: treat this as an edit
                    # REQUEST and validate the editor actually owns the message.
                    index = await db.message_by_id(mid)
                    if (not index or index.get("sender_user_id") != sender_user_id
                            or str(index.get("sender_peer_id")) != str(peer_record["id"])):
                        await ws.send_str(_dumps(_err("edit_message", "Not authorised")))
                        continue

                    mode = channel.get("edit_mode", "off")
                    if mode == "off":
                        await ws.send_str(_dumps(_err("edit_message", "Editing is turned off in this channel")))
                        continue

                    edited_at = datetime.now(timezone.utc)
                    await db.message_index_set_edited(mid, edited_at)
                    # Content for a remote-origin message is never cached here —
                    # nothing local to update or version; the editor's own
                    # server holds the actual text/image and its history.

                    await ws.send_str(_dumps(_ok("edit_message", edited_at=edited_at, edit_mode=mode)))
                else:
                    # Relay: we're just a subscriber, not the host — trust the
                    # upstream peer the same way new_message's relay branch does,
                    # and don't re-fan-out further (only the host does that).
                    if not cid:
                        await ws.send_str(_dumps(_err("edit_message", "Missing channel_id")))
                        continue
                    try:
                        edited_at = datetime.fromisoformat(data.get("edited_at"))
                    except (ValueError, TypeError):
                        edited_at = datetime.now(timezone.utc)
                    await ws.send_str(_dumps(_ok("edit_message")))

                from handlers.ws import broadcast_to_channel, push_to_stream
                bcast_msg = {
                    "id":         str(mid),
                    "channel_id": str(cid),
                    "text":       msg_text,
                    "image":      "📷" if has_image and not msg_text else None,
                    "edited_at":  edited_at.isoformat(),
                    "remote":     True,
                }
                await broadcast_to_channel(cid, {"type": "message_edited", "ok": True, "message": bcast_msg})
                if channel and not channel.get("stream_excluded"):
                    await push_to_stream({"type": "stream_message_edited", "ok": True, "message": {
                        **bcast_msg,
                        "channel_name":   channel["name"],
                        "channel_public": channel["public"],
                    }})
                if channel:
                    asyncio.create_task(notify_peers_of_edit(
                        cid, mid, msg_text, "📷" if has_image else None, edited_at,
                        exclude_peer=peer_record["address"],
                    ))

            elif msg_type == "delete_message":
                peer_record = await _authed_peer(data)
                if not peer_record:
                    await ws.send_str(_dumps(_err("delete_message", "Peer authentication failed")))
                    continue

                mid            = _str_uuid(data.get("message_id"))
                cid            = _str_uuid(data.get("channel_id"))
                sender_user_id = _str_uuid(data.get("sender_user_id"))

                if not mid:
                    await ws.send_str(_dumps(_err("delete_message", "Missing message_id")))
                    continue

                channel = await db.channel_by_id(cid) if cid else None

                if channel:
                    # We host this channel: validate the requester owns the message.
                    index = await db.message_by_id(mid)
                    if (not index or index.get("sender_user_id") != sender_user_id
                            or str(index.get("sender_peer_id")) != str(peer_record["id"])):
                        await ws.send_str(_dumps(_err("delete_message", "Not authorised")))
                        continue
                    await db.message_delete(mid)
                    await ws.send_str(_dumps(_ok("delete_message")))
                else:
                    # Relay: not the host, just forward the push to local clients.
                    if not cid:
                        await ws.send_str(_dumps(_err("delete_message", "Missing channel_id")))
                        continue
                    await ws.send_str(_dumps(_ok("delete_message")))

                from handlers.ws import broadcast_to_channel, push_to_stream
                payload = {"type": "message_deleted", "ok": True,
                           "id": str(mid), "channel_id": str(cid)}
                await broadcast_to_channel(cid, payload)
                if channel and not channel.get("stream_excluded"):
                    await push_to_stream({**payload, "type": "stream_message_deleted"})
                if channel:
                    asyncio.create_task(notify_peers_of_deletion(
                        cid, mid, exclude_peer=peer_record["address"]))

            elif msg_type == "toggle_reaction":
                peer_record = await _authed_peer(data)
                if not peer_record:
                    await ws.send_str(_dumps(_err("toggle_reaction", "Peer authentication failed")))
                    continue

                mid            = _str_uuid(data.get("message_id"))
                cid            = _str_uuid(data.get("channel_id"))
                sender_user_id = _str_uuid(data.get("sender_user_id"))
                emoji          = data.get("emoji")

                if not mid or not cid:
                    await ws.send_str(_dumps(_err("toggle_reaction", "Missing message_id or channel_id")))
                    continue

                channel = await db.channel_by_id(cid)

                if channel:
                    # We host this channel: this is an action request from a remote user.
                    if not emoji or not sender_user_id:
                        await ws.send_str(_dumps(_err("toggle_reaction", "Missing emoji or sender_user_id")))
                        continue
                    if not channel["public"]:
                        if not await db.is_remote_member(cid, sender_user_id, peer_record["id"]):
                            await ws.send_str(_dumps(_err("toggle_reaction", "Not a member of this channel")))
                            continue
                    else:
                        if await db.is_banned(cid, sender_user_id, peer_record["id"]):
                            await ws.send_str(_dumps(_err("toggle_reaction", "You are banned from this channel")))
                            continue
                    if not channel.get("allow_reactions", True):
                        await ws.send_str(_dumps(_err("toggle_reaction", "Reactions are turned off in this channel")))
                        continue
                    await db.reaction_toggle(mid, sender_user_id, peer_record["id"], emoji)
                    reactions = await db.reactions_for_message(mid)
                    await ws.send_str(_dumps(_ok("toggle_reaction", reactions=reactions)))
                else:
                    # Relay: not the host, just forward the push to local clients.
                    reactions = data.get("reactions", [])
                    await ws.send_str(_dumps(_ok("toggle_reaction")))

                from handlers.ws import broadcast_to_channel
                await broadcast_to_channel(cid, {"type": "reaction_updated", "ok": True,
                    "id": str(mid), "channel_id": str(cid), "reactions": reactions})
                if channel:
                    asyncio.create_task(notify_peers_of_reaction(
                        cid, mid, reactions, exclude_peer=peer_record["address"]))

            else:
                await ws.send_str(_dumps(_err(msg_type, f"Unknown type: {msg_type}")))

    except Exception:
        log.exception("Peer WS error from %s", peer_address)
    finally:
        if peer_address:
            _connected_peers.discard(peer_address)

    return ws
