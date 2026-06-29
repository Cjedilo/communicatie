PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS users (
    id          TEXT    PRIMARY KEY,
    name        TEXT    UNIQUE NOT NULL,
    password    TEXT    NOT NULL,
    avatar      TEXT,
    created     TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sessions (
    token       TEXT    PRIMARY KEY,
    user_id     TEXT    NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    expires_at  TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS sessions_expires  ON sessions(expires_at);

CREATE TABLE IF NOT EXISTS peers (
    id              TEXT    PRIMARY KEY,
    name            TEXT,
    address         TEXT    NOT NULL UNIQUE,
    ssl_fingerprint TEXT,
    status          TEXT    NOT NULL DEFAULT 'approved',
    last_seen       TEXT,
    created         TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS channels (
    id              TEXT    PRIMARY KEY,
    name            TEXT    NOT NULL,
    public          INTEGER DEFAULT 1,
    stream_excluded INTEGER DEFAULT 0,
    created_by      TEXT    REFERENCES users(id) ON DELETE SET NULL,
    created         TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS channel_members (
    channel_id  TEXT    NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    user_id     TEXT    NOT NULL,
    peer_id     TEXT    REFERENCES peers(id) ON DELETE CASCADE,
    PRIMARY KEY (channel_id, user_id)
);

CREATE TABLE IF NOT EXISTS message_index (
    id              TEXT    PRIMARY KEY,
    channel_id      TEXT    NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    sender_user_id  TEXT    NOT NULL,
    sender_peer_id  TEXT    REFERENCES peers(id) ON DELETE SET NULL,
    parent_id       TEXT    REFERENCES message_index(id) ON DELETE SET NULL,
    created         TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS msg_idx_channel ON message_index(channel_id, created);
CREATE INDEX IF NOT EXISTS msg_idx_sender  ON message_index(sender_user_id);

CREATE TABLE IF NOT EXISTS message_content (
    id      TEXT    PRIMARY KEY,
    text    TEXT,
    image   TEXT
);

CREATE TABLE IF NOT EXISTS user_cache (
    user_id     TEXT    NOT NULL,
    peer_id     TEXT    REFERENCES peers(id) ON DELETE SET NULL,
    name        TEXT,
    avatar      TEXT,
    updated     TEXT    DEFAULT (datetime('now')),
    PRIMARY KEY (user_id, peer_id)
);

CREATE TABLE IF NOT EXISTS settings (
    key     TEXT PRIMARY KEY,
    value   TEXT
);

-- Per-user preferences (JSON blob, extensible without schema changes)
CREATE TABLE IF NOT EXISTS user_settings (
    user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    data    TEXT NOT NULL DEFAULT '{}'
);

-- Banned users per channel (for public channels)
CREATE TABLE IF NOT EXISTS channel_bans (
    channel_id  TEXT    NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    user_id     TEXT    NOT NULL,
    peer_id     TEXT    REFERENCES peers(id) ON DELETE CASCADE,
    PRIMARY KEY (channel_id, user_id)
);
