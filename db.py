"""
Database abstraction — supports PostgreSQL (asyncpg) and SQLite (aiosqlite).

Set DB_DSN to:
  postgresql://user:pass@host/dbname   → PostgreSQL
  sqlite:///path/to/file.db            → SQLite
"""

import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

import config

# ---------------------------------------------------------------------------
# Type coercion helpers for SQLite (which returns everything as str/int/float)
# ---------------------------------------------------------------------------

_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I
)


def _coerce(v):
    if isinstance(v, str):
        if _UUID_RE.match(v):
            return uuid.UUID(v)
        try:
            return datetime.fromisoformat(v)
        except ValueError:
            pass
    return v


def _coerce_row(row: dict) -> dict:
    return {k: _coerce(v) for k, v in row.items()}


def _sqlite_arg(v):
    """Convert Python value to SQLite-compatible scalar."""
    if isinstance(v, uuid.UUID):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def _pg_to_sqlite(query: str, args: tuple) -> tuple[str, tuple]:
    """
    Translate $1,$2,... to ? and reorder args to match appearance order.
    PostgreSQL allows $N in any order; SQLite ? is strictly positional.
    """
    param_order = [int(m.group(1)) for m in re.finditer(r'\$(\d+)', query)]
    new_query   = re.sub(r'\$\d+', '?', query)
    new_args    = tuple(_sqlite_arg(args[i - 1]) for i in param_order)
    return new_query, new_args


# ---------------------------------------------------------------------------
# Backend classes
# ---------------------------------------------------------------------------

class _PGBackend:
    def __init__(self, dsn: str):
        self._dsn  = dsn
        self._pool = None

    async def init(self):
        import asyncpg
        self._pool = await asyncpg.create_pool(self._dsn)
        schema = (Path(__file__).parent / "schema.sql").read_text()
        async with self._pool.acquire() as c:
            await c.execute(schema)
            await c.execute(
                "ALTER TABLE message_content DROP CONSTRAINT IF EXISTS message_content_id_fkey"
            )

    async def close(self):
        if self._pool:
            await self._pool.close()

    async def fetch(self, query: str, *args) -> list[dict]:
        async with self._pool.acquire() as c:
            return [dict(r) for r in await c.fetch(query, *args)]

    async def fetchrow(self, query: str, *args) -> dict | None:
        async with self._pool.acquire() as c:
            r = await c.fetchrow(query, *args)
            return dict(r) if r else None

    async def execute(self, query: str, *args):
        async with self._pool.acquire() as c:
            await c.execute(query, *args)


class _SQLiteBackend:
    def __init__(self, path: str):
        self._path = path
        self._conn = None

    async def init(self):
        import aiosqlite
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        schema = (Path(__file__).parent / "schema_sqlite.sql").read_text()
        await self._conn.executescript(schema)
        # Migrate: recreate message_content without FK if old schema exists
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS message_content_new (
                id TEXT PRIMARY KEY, text TEXT, image TEXT
            )
        """)
        await self._conn.execute(
            "INSERT OR IGNORE INTO message_content_new SELECT id,text,image FROM message_content"
        )
        await self._conn.execute("DROP TABLE IF EXISTS message_content")
        await self._conn.execute(
            "ALTER TABLE message_content_new RENAME TO message_content"
        )
        await self._conn.commit()

    async def close(self):
        if self._conn:
            await self._conn.close()

    async def fetch(self, query: str, *args) -> list[dict]:
        q, a = _pg_to_sqlite(query, args)
        async with self._conn.execute(q, a) as cur:
            return [_coerce_row(dict(r)) for r in await cur.fetchall()]

    async def fetchrow(self, query: str, *args) -> dict | None:
        q, a = _pg_to_sqlite(query, args)
        async with self._conn.execute(q, a) as cur:
            r = await cur.fetchone()
            return _coerce_row(dict(r)) if r else None

    async def execute(self, query: str, *args):
        q, a = _pg_to_sqlite(query, args)
        await self._conn.execute(q, a)
        await self._conn.commit()


# ---------------------------------------------------------------------------
# Module-level backend instance
# ---------------------------------------------------------------------------

_db: _PGBackend | _SQLiteBackend | None = None
_is_sqlite = False


async def init(app=None):
    global _db, _is_sqlite
    dsn = config.DB_DSN
    if dsn.startswith("sqlite"):
        path     = dsn.removeprefix("sqlite://")
        _db      = _SQLiteBackend(path)
        _is_sqlite = True
    else:
        _db      = _PGBackend(dsn)
        _is_sqlite = False
    await _db.init()


async def close(app=None):
    if _db:
        await _db.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _new_id() -> uuid.UUID:
    return uuid.uuid4()


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

async def setting_get(key: str) -> str | None:
    r = await _db.fetchrow("SELECT value FROM settings WHERE key = $1", key)
    v = r["value"] if r else None
    # Always return a plain string — SQLite coercion may have turned a UUID value
    # into a uuid.UUID object, which would break string comparisons.
    return str(v) if v is not None else None


async def setting_set(key: str, value: str):
    await _db.execute(
        "INSERT INTO settings(key,value) VALUES($1,$2) "
        "ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
        key, value,
    )


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

async def user_create(name: str, password_hash: str) -> dict:
    uid = _new_id()
    if _is_sqlite:
        await _db.execute(
            "INSERT INTO users(id,name,password) VALUES($1,$2,$3)",
            uid, name, password_hash,
        )
        return await _db.fetchrow("SELECT * FROM users WHERE id=$1", uid)
    return await _db.fetchrow(
        "INSERT INTO users(id,name,password) VALUES($1,$2,$3) RETURNING *",
        uid, name, password_hash,
    )


async def user_by_name(name: str) -> dict | None:
    return await _db.fetchrow("SELECT * FROM users WHERE name=$1", name)


async def user_by_id(uid: uuid.UUID) -> dict | None:
    return await _db.fetchrow("SELECT * FROM users WHERE id=$1", uid)


async def user_set_password(uid: uuid.UUID, password_hash: str):
    await _db.execute("UPDATE users SET password=$2 WHERE id=$1", uid, password_hash)


async def user_set_avatar(uid: uuid.UUID, path: str):
    await _db.execute("UPDATE users SET avatar=$2 WHERE id=$1", uid, path)


async def user_delete(uid: uuid.UUID):
    await _db.execute("DELETE FROM users WHERE id=$1", uid)


async def users_all() -> list[dict]:
    return await _db.fetch(
        "SELECT id, name, avatar, created FROM users ORDER BY name"
    )


async def users_by_ids(ids: list[uuid.UUID]) -> list[dict]:
    if not ids:
        return []
    if _is_sqlite:
        placeholders = ",".join(f"${i+1}" for i in range(len(ids)))
        return await _db.fetch(
            f"SELECT id, name, avatar, created FROM users WHERE id IN ({placeholders})",
            *ids,
        )
    return await _db.fetch(
        "SELECT id, name, avatar, created FROM users WHERE id = ANY($1::uuid[])", ids
    )


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

async def session_create(token: str, user_id: uuid.UUID, expires_at: datetime):
    await _db.execute(
        "INSERT INTO sessions(token,user_id,expires_at) VALUES($1,$2,$3)",
        token, user_id, expires_at,
    )


async def session_get(token: str) -> dict | None:
    return await _db.fetchrow(
        "SELECT s.*, u.name, u.avatar FROM sessions s "
        "JOIN users u ON u.id = s.user_id "
        "WHERE s.token=$1 AND s.expires_at > $2",
        token, _now(),
    )


async def session_delete(token: str):
    await _db.execute("DELETE FROM sessions WHERE token=$1", token)


async def sessions_purge_expired():
    await _db.execute("DELETE FROM sessions WHERE expires_at <= $1", _now())


# ---------------------------------------------------------------------------
# Channels
# ---------------------------------------------------------------------------

async def channel_create(name: str, public: bool, created_by: uuid.UUID) -> dict:
    cid = _new_id()
    pub = 1 if (_is_sqlite and public) else public
    if _is_sqlite:
        await _db.execute(
            "INSERT INTO channels(id,name,public,created_by) VALUES($1,$2,$3,$4)",
            cid, name, pub, created_by,
        )
        return await _db.fetchrow("SELECT * FROM channels WHERE id=$1", cid)
    return await _db.fetchrow(
        "INSERT INTO channels(id,name,public,created_by) VALUES($1,$2,$3,$4) RETURNING *",
        cid, name, public, created_by,
    )


async def channel_by_id(cid: uuid.UUID) -> dict | None:
    return await _db.fetchrow("SELECT * FROM channels WHERE id=$1", cid)


async def channel_delete(cid: uuid.UUID):
    await _db.execute("DELETE FROM channels WHERE id=$1", cid)


async def channels_visible_to(user_id: uuid.UUID) -> list[dict]:
    return await _db.fetch(
        """
        SELECT DISTINCT c.*
        FROM channels c
        LEFT JOIN channel_members cm ON cm.channel_id = c.id AND cm.user_id = $1
        WHERE c.public = $2 OR cm.user_id IS NOT NULL
        ORDER BY c.name
        """,
        user_id, 1 if _is_sqlite else True,
    )


async def channels_all_public() -> list[dict]:
    return await _db.fetch(
        "SELECT * FROM channels WHERE public=$1 ORDER BY name",
        1 if _is_sqlite else True,
    )


async def channels_visible_to_peer(peer_id: uuid.UUID) -> list[dict]:
    """Public channels + private channels where this peer has at least one member."""
    return await _db.fetch(
        """
        SELECT DISTINCT c.*
        FROM channels c
        LEFT JOIN channel_members cm ON cm.channel_id = c.id AND cm.peer_id = $1
        WHERE c.public = $2 OR cm.peer_id IS NOT NULL
        ORDER BY c.name
        """,
        peer_id, 1 if _is_sqlite else True,
    )


# ---------------------------------------------------------------------------
# Channel members
# ---------------------------------------------------------------------------

async def member_add(channel_id: uuid.UUID, user_id: uuid.UUID, peer_id: uuid.UUID | None = None):
    await _db.execute(
        "INSERT INTO channel_members(channel_id,user_id,peer_id) VALUES($1,$2,$3) "
        "ON CONFLICT DO NOTHING",
        channel_id, user_id, peer_id,
    )


async def member_remove(channel_id: uuid.UUID, user_id: uuid.UUID):
    await _db.execute(
        "DELETE FROM channel_members WHERE channel_id=$1 AND user_id=$2",
        channel_id, user_id,
    )


async def members_of(channel_id: uuid.UUID) -> list[dict]:
    return await _db.fetch(
        """
        SELECT cm.*,
               COALESCE(u.name,   uc.name)   AS name,
               COALESCE(u.avatar, uc.avatar) AS avatar,
               p.name    AS peer_name,
               p.address AS peer_address
        FROM channel_members cm
        LEFT JOIN users      u  ON u.id        = cm.user_id AND cm.peer_id IS NULL
        LEFT JOIN user_cache uc ON uc.user_id  = cm.user_id AND uc.peer_id = cm.peer_id
        LEFT JOIN peers      p  ON p.id        = cm.peer_id
        WHERE cm.channel_id = $1
        """,
        channel_id,
    )


async def is_member(channel_id: uuid.UUID, user_id: uuid.UUID) -> bool:
    r = await _db.fetchrow(
        "SELECT 1 FROM channel_members WHERE channel_id=$1 AND user_id=$2",
        channel_id, user_id,
    )
    return r is not None


async def peer_has_member_in_channel(channel_id: uuid.UUID, peer_id: uuid.UUID) -> bool:
    """True if the peer has at least one user who is a member of this channel."""
    r = await _db.fetchrow(
        "SELECT 1 FROM channel_members WHERE channel_id=$1 AND peer_id=$2",
        channel_id, peer_id,
    )
    return r is not None


async def is_remote_member(channel_id: uuid.UUID, user_id: uuid.UUID, peer_id: uuid.UUID) -> bool:
    """True if this specific remote user (from peer_id) is a member of the channel."""
    r = await _db.fetchrow(
        "SELECT 1 FROM channel_members WHERE channel_id=$1 AND user_id=$2 AND peer_id=$3",
        channel_id, user_id, peer_id,
    )
    return r is not None


# ---------------------------------------------------------------------------
# Message index
# ---------------------------------------------------------------------------

async def message_index_add(
    msg_id: uuid.UUID,
    channel_id: uuid.UUID,
    sender_user_id: uuid.UUID,
    sender_peer_id: uuid.UUID | None,
    parent_id: uuid.UUID | None,
    created: datetime,
):
    await _db.execute(
        "INSERT INTO message_index"
        "(id,channel_id,sender_user_id,sender_peer_id,parent_id,created) "
        "VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",
        msg_id, channel_id, sender_user_id, sender_peer_id, parent_id, created,
    )


async def messages_for_channel(channel_id: uuid.UUID, limit: int = 100) -> list[dict]:
    return await _db.fetch(
        """
        SELECT mi.*, mc.text, mc.image,
               COALESCE(u.name,   uc.name)   AS sender_name,
               COALESCE(u.avatar, uc.avatar) AS sender_avatar,
               p.address AS peer_address, p.name AS peer_name
        FROM message_index mi
        LEFT JOIN message_content mc ON mc.id = mi.id
        LEFT JOIN users      u  ON u.id  = mi.sender_user_id AND mi.sender_peer_id IS NULL
        LEFT JOIN user_cache uc ON uc.user_id = mi.sender_user_id AND uc.peer_id = mi.sender_peer_id
        LEFT JOIN peers      p  ON p.id  = mi.sender_peer_id
        WHERE mi.channel_id = $1
        ORDER BY mi.created ASC
        LIMIT $2
        """,
        channel_id, limit,
    )


async def message_by_id(msg_id: uuid.UUID) -> dict | None:
    return await _db.fetchrow(
        """
        SELECT mi.*, mc.text, mc.image,
               u.name AS sender_name, u.avatar AS sender_avatar
        FROM message_index mi
        LEFT JOIN message_content mc ON mc.id = mi.id
        LEFT JOIN users u ON u.id = mi.sender_user_id AND mi.sender_peer_id IS NULL
        WHERE mi.id = $1
        """,
        msg_id,
    )


# ---------------------------------------------------------------------------
# Message content
# ---------------------------------------------------------------------------

async def user_cache_upsert(user_id: uuid.UUID, peer_id: uuid.UUID, name: str | None, avatar: str | None):
    await _db.execute(
        "INSERT INTO user_cache(user_id, peer_id, name, avatar) VALUES($1,$2,$3,$4) "
        "ON CONFLICT(user_id, peer_id) DO UPDATE SET name=EXCLUDED.name, avatar=EXCLUDED.avatar, updated=$5",
        user_id, peer_id, name, avatar, _now(),
    )


async def message_content_add(msg_id: uuid.UUID, text: str | None, image: str | None):
    await _db.execute(
        "INSERT INTO message_content(id,text,image) VALUES($1,$2,$3)",
        msg_id, text, image,
    )


async def message_content_local(msg_id: uuid.UUID) -> dict | None:
    """Return content if this server is the origin (sender stored it here)."""
    return await _db.fetchrow(
        "SELECT id, text, image FROM message_content WHERE id=$1", msg_id
    )


# ---------------------------------------------------------------------------
# Peers
# ---------------------------------------------------------------------------

async def peer_upsert(address: str, name: str | None, fingerprint: str | None) -> dict:
    pid = _new_id()
    if _is_sqlite:
        await _db.execute(
            "INSERT INTO peers(id,address,name,ssl_fingerprint) VALUES($1,$2,$3,$4) "
            "ON CONFLICT(address) DO UPDATE "
            "SET name=COALESCE(EXCLUDED.name, peers.name), "
            "    ssl_fingerprint=COALESCE(EXCLUDED.ssl_fingerprint, peers.ssl_fingerprint)",
            pid, address, name, fingerprint,
        )
        return await _db.fetchrow("SELECT * FROM peers WHERE address=$1", address)
    return await _db.fetchrow(
        """
        INSERT INTO peers(id,address,name,ssl_fingerprint) VALUES($1,$2,$3,$4)
        ON CONFLICT(address) DO UPDATE
            SET name=COALESCE(EXCLUDED.name, peers.name),
                ssl_fingerprint=COALESCE(EXCLUDED.ssl_fingerprint, peers.ssl_fingerprint)
        RETURNING *
        """,
        pid, address, name, fingerprint,
    )


async def peer_by_id(pid: uuid.UUID) -> dict | None:
    return await _db.fetchrow("SELECT * FROM peers WHERE id=$1", pid)


async def peer_by_address(address: str) -> dict | None:
    return await _db.fetchrow("SELECT * FROM peers WHERE address=$1", address)


async def peers_all() -> list[dict]:
    return await _db.fetch("SELECT * FROM peers ORDER BY name")


async def peer_delete(pid: uuid.UUID):
    await _db.execute("DELETE FROM peers WHERE id=$1", pid)
