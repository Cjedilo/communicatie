CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS users (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT        UNIQUE NOT NULL,
    password    TEXT        NOT NULL,   -- argon2 hash
    avatar      TEXT,
    created     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sessions (
    token       TEXT        PRIMARY KEY,
    user_id     UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    expires_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS sessions_expires  ON sessions(expires_at);

-- Peers (other server instances we federate with)
CREATE TABLE IF NOT EXISTS peers (
    id              UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT,
    address         TEXT    NOT NULL UNIQUE,    -- wss://host:port
    ssl_fingerprint TEXT,                       -- SHA-256 of their cert, pinned on first connect
    created         TIMESTAMPTZ DEFAULT NOW()
);

-- Channels are owned by this server
CREATE TABLE IF NOT EXISTS channels (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT        NOT NULL,
    public      BOOLEAN     DEFAULT TRUE,
    created_by  UUID        REFERENCES users(id) ON DELETE SET NULL,
    created     TIMESTAMPTZ DEFAULT NOW()
);

-- Members of private channels.
-- user_id non-null  + peer_id null   → local user
-- user_id non-null  + peer_id non-null → remote user on that peer
CREATE TABLE IF NOT EXISTS channel_members (
    channel_id  UUID    NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    user_id     UUID    NOT NULL,
    peer_id     UUID    REFERENCES peers(id) ON DELETE CASCADE,
    PRIMARY KEY (channel_id, user_id)
);

-- Message index: structure only, content lives on the sender's server
-- For local messages sender_peer_id IS NULL and content is stored below.
CREATE TABLE IF NOT EXISTS message_index (
    id              UUID        PRIMARY KEY,
    channel_id      UUID        NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    sender_user_id  UUID        NOT NULL,
    sender_peer_id  UUID        REFERENCES peers(id) ON DELETE SET NULL,
    sender_name     TEXT,
    sender_avatar   TEXT,
    parent_id       UUID        REFERENCES message_index(id) ON DELETE SET NULL,
    created         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS msg_idx_channel  ON message_index(channel_id, created);
CREATE INDEX IF NOT EXISTS msg_idx_sender   ON message_index(sender_user_id);

-- Content for messages sent by local users on this server
CREATE TABLE IF NOT EXISTS message_content (
    id      UUID    PRIMARY KEY,
    text    TEXT,
    image   TEXT
);

-- Profile cache for users from federated servers
CREATE TABLE IF NOT EXISTS user_cache (
    user_id     UUID    NOT NULL,
    peer_id     UUID    REFERENCES peers(id) ON DELETE CASCADE,
    name        TEXT,
    avatar      TEXT,
    updated     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, peer_id)
);

-- Server-wide key/value settings
-- Keys: peer_name, peer_address, owner_id, letsencrypt_domain, letsencrypt_email
CREATE TABLE IF NOT EXISTS settings (
    key     TEXT PRIMARY KEY,
    value   TEXT
);
