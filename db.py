"""
Database abstraction — supports PostgreSQL (asyncpg) and SQLite (aiosqlite).

Set DB_DSN to:
  postgresql://user:pass@host/dbname   → PostgreSQL
  sqlite:///path/to/file.db            → SQLite
"""

import json
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
            await c.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name TEXT"
            )
            await c.execute(
                "ALTER TABLE channels ADD COLUMN IF NOT EXISTS icon TEXT"
            )
            await c.execute(
                "ALTER TABLE peers ADD COLUMN IF NOT EXISTS auth_token TEXT"
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
        # Migrate: add new columns if missing
        for col_sql in [
            "ALTER TABLE peers ADD COLUMN status TEXT NOT NULL DEFAULT 'approved'",
            "ALTER TABLE peers ADD COLUMN last_seen TEXT",
            "ALTER TABLE peers ADD COLUMN auth_token TEXT",
            "ALTER TABLE users ADD COLUMN display_name TEXT",
            "ALTER TABLE channels ADD COLUMN icon TEXT",
        ]:
            try:
                await self._conn.execute(col_sql)
                await self._conn.commit()
            except Exception:
                pass  # column already exists

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
    # One-time migration: set stream_excluded=True for existing public channels.
    # Guarded by a settings flag so user overrides are never reset on restart.
    # One-time cleanup: remove cached remote message content (fetch-on-demand now)
    if not await _db.fetchrow("SELECT value FROM settings WHERE key=$1", "purged_remote_content"):
        await purge_remote_message_content()
        await _db.execute(
            "INSERT INTO settings(key,value) VALUES($1,$2) ON CONFLICT(key) DO NOTHING",
            "purged_remote_content", "1",
        )

    if not await _db.fetchrow("SELECT value FROM settings WHERE key=$1", "migrated_stream_excluded"):
        t = 1 if _is_sqlite else True
        f = 0 if _is_sqlite else False
        await _db.execute(
            "UPDATE channels SET stream_excluded=$1 WHERE public=$2 AND (stream_excluded=$3 OR stream_excluded IS NULL)",
            t, t, f,
        )
        await _db.execute(
            "INSERT INTO settings(key,value) VALUES($1,$2) ON CONFLICT(key) DO NOTHING",
            "migrated_stream_excluded", "1",
        )


async def purge_remote_message_content():
    """Delete cached message_content for messages not sent by local users.
    Keeps only content where this server is the origin (sender_peer_id IS NULL).
    """
    await _db.execute(
        """
        DELETE FROM message_content
        WHERE id IN (
            SELECT mc.id FROM message_content mc
            JOIN message_index mi ON mi.id = mc.id
            WHERE mi.sender_peer_id IS NOT NULL
        )
        """
    )
    # Also remove orphaned content with no message_index at all
    await _db.execute(
        """
        DELETE FROM message_content
        WHERE id NOT IN (SELECT id FROM message_index)
        """
    )


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
# User settings (per-user JSON blob)
# ---------------------------------------------------------------------------

async def user_settings_get(user_id: uuid.UUID) -> dict:
    row = await _db.fetchrow("SELECT data FROM user_settings WHERE user_id=$1", user_id)
    if not row or not row.get("data"):
        return {}
    data = row["data"]
    try:
        return json.loads(data) if isinstance(data, str) else dict(data)
    except Exception:
        return {}


async def user_setting_set(user_id: uuid.UUID, key: str, value) -> None:
    settings = await user_settings_get(user_id)
    settings[key] = value
    data_str = json.dumps(settings)
    await _db.execute(
        "INSERT INTO user_settings(user_id, data) VALUES($1,$2) "
        "ON CONFLICT(user_id) DO UPDATE SET data=$2",
        user_id, data_str,
    )


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
    # sender_user_id has no FK to users (it's polymorphic: local or remote
    # depending on sender_peer_id), so cascade content deletion by hand —
    # only for this user's own local messages (sender_peer_id IS NULL).
    await _db.execute(
        "DELETE FROM message_content WHERE id IN "
        "(SELECT id FROM message_index WHERE sender_user_id=$1 AND sender_peer_id IS NULL)",
        uid,
    )
    await _db.execute(
        "DELETE FROM message_index WHERE sender_user_id=$1 AND sender_peer_id IS NULL", uid
    )
    await _db.execute("DELETE FROM users WHERE id=$1", uid)


async def users_all() -> list[dict]:
    return await _db.fetch(
        "SELECT id, COALESCE(display_name, name) AS name, avatar, created FROM users ORDER BY name"
    )


async def users_by_ids(ids: list[uuid.UUID]) -> list[dict]:
    if not ids:
        return []
    if _is_sqlite:
        placeholders = ",".join(f"${i+1}" for i in range(len(ids)))
        return await _db.fetch(
            f"SELECT id, COALESCE(display_name, name) AS name, avatar, created FROM users WHERE id IN ({placeholders})",
            *ids,
        )
    return await _db.fetch(
        "SELECT id, COALESCE(display_name, name) AS name, avatar, created FROM users WHERE id = ANY($1::uuid[])", ids
    )


async def channel_set_icon(channel_id: uuid.UUID, icon: str | None):
    await _db.execute("UPDATE channels SET icon=$2 WHERE id=$1", channel_id, icon or None)


async def user_set_display_name(uid: uuid.UUID, display_name: str | None):
    await _db.execute("UPDATE users SET display_name=$2 WHERE id=$1", uid, display_name or None)


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
        "SELECT s.*, COALESCE(u.display_name, u.name) AS name, u.avatar FROM sessions s "
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
    cid  = _new_id()
    pub  = 1 if (_is_sqlite and public) else public
    excl = 1 if (_is_sqlite and public) else public  # public → excluded from stream by default
    if _is_sqlite:
        await _db.execute(
            "INSERT INTO channels(id,name,public,created_by,stream_excluded) VALUES($1,$2,$3,$4,$5)",
            cid, name, pub, created_by, excl,
        )
        return await _db.fetchrow("SELECT * FROM channels WHERE id=$1", cid)
    return await _db.fetchrow(
        "INSERT INTO channels(id,name,public,created_by,stream_excluded) VALUES($1,$2,$3,$4,$5) RETURNING *",
        cid, name, public, created_by, public,
    )


async def channel_by_id(cid: uuid.UUID) -> dict | None:
    return await _db.fetchrow("SELECT * FROM channels WHERE id=$1", cid)


async def channel_delete(cid: uuid.UUID):
    await _db.execute("DELETE FROM channels WHERE id=$1", cid)


async def stream_messages(user_id: uuid.UUID, limit: int = 60, before=None) -> list[dict]:
    """Recent messages across all visible, non-excluded local channels."""
    before_clause = "AND mi.created < $5" if before is not None else ""
    extra_args    = (before,) if before is not None else ()
    return await _db.fetch(
        f"""
        SELECT mi.*, mc.text, mc.image,
               COALESCE(COALESCE(u.display_name, u.name), uc.name,
                   (SELECT name FROM user_cache
                    WHERE user_id = mi.sender_user_id LIMIT 1))  AS sender_name,
               COALESCE(u.avatar, uc.avatar,
                   (SELECT avatar FROM user_cache
                    WHERE user_id = mi.sender_user_id LIMIT 1)) AS sender_avatar,
               c.name            AS channel_name,
               c.icon            AS channel_icon,
               c.public          AS channel_public,
               c.stream_excluded AS stream_excluded,
               p.address         AS peer_address,
               p.name            AS peer_name
        FROM message_index mi
        JOIN channels c ON c.id = mi.channel_id
        LEFT JOIN channel_members cm
            ON cm.channel_id = c.id AND cm.user_id = $1
        LEFT JOIN message_content mc ON mc.id = mi.id
        LEFT JOIN users u
            ON u.id = mi.sender_user_id AND mi.sender_peer_id IS NULL
        LEFT JOIN user_cache uc
            ON uc.user_id = mi.sender_user_id AND uc.peer_id = mi.sender_peer_id
        LEFT JOIN peers p ON p.id = mi.sender_peer_id
        WHERE (c.public = $2 OR cm.user_id IS NOT NULL)
          AND (c.stream_excluded = $3 OR c.stream_excluded IS NULL)
          {before_clause}
        ORDER BY mi.created DESC
        LIMIT $4
        """,
        user_id,
        1 if _is_sqlite else True,
        0 if _is_sqlite else False,
        limit,
        *extra_args,
    )


async def channel_last_message_summary(channel_id: uuid.UUID) -> dict | None:
    """Returns last_activity timestamp and last_sender_name for a channel."""
    return await _db.fetchrow(
        """
        SELECT mi.created AS last_activity,
               COALESCE(COALESCE(u.display_name, u.name), uc.name) AS last_sender_name
        FROM message_index mi
        LEFT JOIN users u
            ON u.id = mi.sender_user_id AND mi.sender_peer_id IS NULL
        LEFT JOIN user_cache uc
            ON uc.user_id = mi.sender_user_id AND uc.peer_id = mi.sender_peer_id
        WHERE mi.channel_id = $1
        ORDER BY mi.created DESC LIMIT 1
        """,
        channel_id,
    )


async def channel_set_stream_excluded(channel_id: uuid.UUID, excluded: bool):
    val = 1 if (_is_sqlite and excluded) else excluded
    await _db.execute(
        "UPDATE channels SET stream_excluded=$2 WHERE id=$1",
        channel_id, val,
    )


async def channels_stream(user_id: uuid.UUID) -> list[dict]:
    """Channels visible to user with last-message preview, sorted by last activity."""
    return await _db.fetch(
        """
        SELECT DISTINCT c.*,
            last_mi.created          AS last_activity,
            last_mi.sender_peer_id   AS last_peer_id,
            COALESCE(COALESCE(u.display_name, u.name), uc.name) AS last_sender_name,
            mc.text                  AS last_text,
            mc.image                 AS last_image,
            p.name                   AS last_peer_name
        FROM channels c
        LEFT JOIN channel_members cm
            ON cm.channel_id = c.id AND cm.user_id = $1
        LEFT JOIN message_index last_mi
            ON last_mi.id = (
                SELECT id FROM message_index mi2
                WHERE mi2.channel_id = c.id
                ORDER BY mi2.created DESC LIMIT 1
            )
        LEFT JOIN message_content mc ON mc.id = last_mi.id
        LEFT JOIN users u
            ON u.id = last_mi.sender_user_id AND last_mi.sender_peer_id IS NULL
        LEFT JOIN user_cache uc
            ON uc.user_id = last_mi.sender_user_id AND uc.peer_id = last_mi.sender_peer_id
        LEFT JOIN peers p ON p.id = last_mi.sender_peer_id
        WHERE c.public = $2 OR cm.user_id IS NOT NULL
        ORDER BY last_activity DESC NULLS LAST, c.name
        """,
        user_id, 1 if _is_sqlite else True,
    )


async def channel_direct_find(user_id_a: uuid.UUID, user_id_b: uuid.UUID,
                              peer_id_b: uuid.UUID | None = None) -> dict | None:
    """Find a private channel whose only members are exactly these two users."""
    is_self  = str(user_id_a) == str(user_id_b) and peer_id_b is None
    expected = 1 if is_self else 2
    pub      = 0 if _is_sqlite else False
    row = await _db.fetchrow(
        """
        SELECT c.id FROM channels c
        JOIN channel_members cm1
             ON cm1.channel_id = c.id AND cm1.user_id = $1 AND cm1.peer_id IS NULL
        JOIN channel_members cm2
             ON cm2.channel_id = c.id AND cm2.user_id = $2
             AND (cm2.peer_id = $3 OR ($3 IS NULL AND cm2.peer_id IS NULL))
        WHERE c.public = $4
          AND (SELECT COUNT(*) FROM channel_members WHERE channel_id = c.id) = $5
        LIMIT 1
        """,
        user_id_a, user_id_b, peer_id_b, pub, expected,
    )
    return await channel_by_id(row["id"]) if row else None


async def channels_visible_to(user_id: uuid.UUID) -> list[dict]:
    return await _db.fetch(
        """
        SELECT DISTINCT c.*, u.name AS created_by_name
        FROM channels c
        LEFT JOIN channel_members cm ON cm.channel_id = c.id AND cm.user_id = $1
        LEFT JOIN users u ON u.id = c.created_by
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


async def channels_visible_to_remote_user(user_id: uuid.UUID, peer_id: uuid.UUID) -> list[dict]:
    """Public channels + private channels where THIS specific remote user is a member."""
    return await _db.fetch(
        """
        SELECT DISTINCT c.*
        FROM channels c
        LEFT JOIN channel_members cm
            ON cm.channel_id = c.id AND cm.user_id = $1 AND cm.peer_id = $2
        WHERE c.public = $3 OR cm.user_id IS NOT NULL
        ORDER BY c.name
        """,
        user_id, peer_id, 1 if _is_sqlite else True,
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
               COALESCE(COALESCE(u.display_name, u.name), uc.name) AS name,
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


# ---------------------------------------------------------------------------
# Channel bans (public channels)
# ---------------------------------------------------------------------------

async def channel_participants(channel_id: uuid.UUID) -> list[dict]:
    """Unique senders who have posted at least one message in this channel."""
    rows = await _db.fetch(
        """
        SELECT DISTINCT mi.sender_user_id AS user_id, mi.sender_peer_id AS peer_id,
               COALESCE(COALESCE(u.display_name, u.name), uc.name) AS name,
               COALESCE(u.avatar,  uc.avatar) AS avatar,
               p.name AS peer_name
        FROM message_index mi
        LEFT JOIN users      u  ON u.id       = mi.sender_user_id AND mi.sender_peer_id IS NULL
        LEFT JOIN user_cache uc ON uc.user_id = mi.sender_user_id AND uc.peer_id = mi.sender_peer_id
        LEFT JOIN peers      p  ON p.id       = mi.sender_peer_id
        WHERE mi.channel_id = $1
        """,
        channel_id,
    )
    for r in rows:
        r["id"] = r["user_id"]
    return rows


async def channel_ban_add(channel_id: uuid.UUID, user_id: uuid.UUID, peer_id: uuid.UUID | None = None):
    await _db.execute(
        "INSERT INTO channel_bans(channel_id,user_id,peer_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
        channel_id, user_id, peer_id,
    )


async def channel_ban_remove(channel_id: uuid.UUID, user_id: uuid.UUID):
    await _db.execute(
        "DELETE FROM channel_bans WHERE channel_id=$1 AND user_id=$2",
        channel_id, user_id,
    )


async def channel_bans_for(channel_id: uuid.UUID) -> list[dict]:
    rows = await _db.fetch(
        """
        SELECT cb.user_id, cb.peer_id,
               COALESCE(COALESCE(u.display_name, u.name), uc.name) AS name,
               COALESCE(u.avatar, uc.avatar) AS avatar,
               p.name AS peer_name
        FROM channel_bans cb
        LEFT JOIN users      u  ON u.id       = cb.user_id AND cb.peer_id IS NULL
        LEFT JOIN user_cache uc ON uc.user_id = cb.user_id AND uc.peer_id = cb.peer_id
        LEFT JOIN peers      p  ON p.id       = cb.peer_id
        WHERE cb.channel_id = $1
        """,
        channel_id,
    )
    for r in rows:
        r["id"] = r["user_id"]
    return rows


async def is_banned(channel_id: uuid.UUID, user_id: uuid.UUID, peer_id: uuid.UUID | None = None) -> bool:
    # Match the ban to the exact identity: a local user (peer_id IS NULL) and a
    # remote user with the same user_id from some peer are distinct principals.
    r = await _db.fetchrow(
        "SELECT 1 FROM channel_bans WHERE channel_id=$1 AND user_id=$2 "
        "AND (peer_id = $3 OR (peer_id IS NULL AND $3 IS NULL))",
        channel_id, user_id, peer_id,
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


async def messages_by_user_for_peer(
    sender_user_id: uuid.UUID,
    requesting_peer_id: uuid.UUID | None,
    limit: int = 10,
) -> list[dict]:
    """Recent messages by a local user, filtered to what a peer server may see."""
    pub = 1 if _is_sqlite else True
    return await _db.fetch(
        """
        SELECT mi.id, mi.channel_id, mi.created,
               mc.text, mc.image,
               c.name AS channel_name, c.public AS channel_public, c.icon AS channel_icon
        FROM message_index mi
        JOIN channels c ON c.id = mi.channel_id
        LEFT JOIN channel_members cm
            ON cm.channel_id = c.id AND cm.peer_id = $2
        LEFT JOIN message_content mc ON mc.id = mi.id
        WHERE mi.sender_user_id = $1
          AND mi.sender_peer_id IS NULL
          AND (c.public = $3 OR cm.peer_id IS NOT NULL)
        ORDER BY mi.created DESC
        LIMIT $4
        """,
        sender_user_id, requesting_peer_id, pub, limit,
    )


async def messages_by_user(
    sender_user_id: uuid.UUID,
    viewer_user_id: uuid.UUID,
    sender_peer_id: uuid.UUID | None = None,
    limit: int = 10,
) -> list[dict]:
    """Recent messages by a user, only from channels the viewer can access."""
    pub = 1 if _is_sqlite else True
    return await _db.fetch(
        """
        SELECT mi.id, mi.channel_id, mi.created,
               mc.text, mc.image,
               c.name AS channel_name, c.public AS channel_public, c.icon AS channel_icon
        FROM message_index mi
        JOIN channels c ON c.id = mi.channel_id
        LEFT JOIN channel_members cm
            ON cm.channel_id = c.id AND cm.user_id = $2
        LEFT JOIN message_content mc ON mc.id = mi.id
        WHERE mi.sender_user_id = $1
          AND (mi.sender_peer_id = $3 OR ($3 IS NULL AND mi.sender_peer_id IS NULL))
          AND (c.public = $4 OR cm.user_id IS NOT NULL)
        ORDER BY mi.created DESC
        LIMIT $5
        """,
        sender_user_id, viewer_user_id, sender_peer_id, pub, limit,
    )


async def messages_for_channel(channel_id: uuid.UUID, limit: int = 100, before=None) -> list[dict]:
    before_clause = "AND mi.created < $3" if before is not None else ""
    extra_args    = (before,) if before is not None else ()
    # With `before`: DESC so we get the *newest* messages before the cursor (stream pagination).
    # Without `before`: ASC for the normal chat view.
    order = "DESC" if before is not None else "ASC"
    return await _db.fetch(
        f"""
        SELECT mi.*, mc.text, mc.image,
               COALESCE(COALESCE(u.display_name, u.name), uc.name,
                   (SELECT name FROM user_cache
                    WHERE user_id = mi.sender_user_id LIMIT 1))   AS sender_name,
               COALESCE(u.avatar, uc.avatar,
                   (SELECT avatar FROM user_cache
                    WHERE user_id = mi.sender_user_id LIMIT 1)) AS sender_avatar,
               p.address AS peer_address, p.name AS peer_name
        FROM message_index mi
        LEFT JOIN message_content mc ON mc.id = mi.id
        LEFT JOIN users      u  ON u.id  = mi.sender_user_id AND mi.sender_peer_id IS NULL
        LEFT JOIN user_cache uc ON uc.user_id = mi.sender_user_id AND uc.peer_id = mi.sender_peer_id
        LEFT JOIN peers      p  ON p.id  = mi.sender_peer_id
        WHERE mi.channel_id = $1 {before_clause}
        ORDER BY mi.created {order}
        LIMIT $2
        """,
        channel_id, limit, *extra_args,
    )


async def message_by_id(msg_id: uuid.UUID) -> dict | None:
    return await _db.fetchrow(
        """
        SELECT mi.*, mc.text, mc.image,
               COALESCE(u.display_name, u.name) AS sender_name, u.avatar AS sender_avatar
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

async def user_cache_get(user_id: uuid.UUID, peer_id: uuid.UUID) -> dict | None:
    return await _db.fetchrow(
        "SELECT * FROM user_cache WHERE user_id=$1 AND peer_id=$2", user_id, peer_id
    )


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
    # Deduplicate by fingerprint: same cert = same server, update address if changed
    if fingerprint:
        existing = await _db.fetchrow(
            "SELECT * FROM peers WHERE ssl_fingerprint=$1", fingerprint
        )
        if existing:
            await _db.execute(
                "UPDATE peers SET address=$1, name=COALESCE($2, name) WHERE ssl_fingerprint=$3",
                address, name, fingerprint,
            )
            return await _db.fetchrow("SELECT * FROM peers WHERE ssl_fingerprint=$1", fingerprint)

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


async def peer_set_status(peer_id: uuid.UUID, status: str):
    await _db.execute("UPDATE peers SET status=$2 WHERE id=$1", peer_id, status)


async def peer_set_auth_token(peer_id: uuid.UUID, token: str | None):
    """Set the shared secret used to authenticate this peer's federation requests."""
    await _db.execute("UPDATE peers SET auth_token=$2 WHERE id=$1", peer_id, token)


async def peer_update_last_seen(address: str):
    await _db.execute(
        "UPDATE peers SET last_seen=$2 WHERE address=$1",
        address, _now(),
    )


async def peer_by_id(pid: uuid.UUID) -> dict | None:
    return await _db.fetchrow("SELECT * FROM peers WHERE id=$1", pid)


async def peer_by_address(address: str) -> dict | None:
    return await _db.fetchrow("SELECT * FROM peers WHERE address=$1", address)


async def peers_all() -> list[dict]:
    return await _db.fetch("SELECT * FROM peers ORDER BY name")


async def peer_block(pid: uuid.UUID):
    """Soft-delete: block the peer so they can't connect but history is preserved."""
    await _db.execute("UPDATE peers SET status='blocked' WHERE id=$1", pid)


async def peer_delete(pid: uuid.UUID):
    """Hard delete — only call when peer is already blocked."""
    await _db.execute("UPDATE peers SET ssl_fingerprint=NULL WHERE id=$1", pid)
    await _db.execute("DELETE FROM peers WHERE id=$1", pid)
