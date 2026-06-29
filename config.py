import os
import db_config

# Database
# If env gives SQLite it is authoritative — the instance owns its own DB.
# Otherwise the UI-configured value (stored in _config.db) takes precedence.
_env_dsn = os.getenv("DB_DSN", "")
if _env_dsn.startswith("sqlite"):
    DB_DSN = _env_dsn
else:
    DB_DSN = (
        db_config.cfg_get("db_dsn")
        or _env_dsn
        or "postgresql://communicatie:communicatie@localhost/communicatie"
    )

# Base path — derived from peer_address path component, e.g. "/chat"
# Must be set before first request; requires restart to change.
BASE_PATH = (db_config.cfg_get("base_path") or os.getenv("BASE_PATH", "")).rstrip("/")
# Note: default password is 'communicatie' (double-m)

# Server
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "443"))
PORT_HTTP = int(os.getenv("PORT_HTTP", "80"))

# SSL
_base    = os.path.dirname(os.path.abspath(__file__))
SSL_CERT = db_config.cfg_get("ssl_cert") or os.getenv("SSL_CERT", os.path.join(_base, "ssl", "cert.pem"))
SSL_KEY  = db_config.cfg_get("ssl_key")  or os.getenv("SSL_KEY",  os.path.join(_base, "ssl", "key.pem"))
SSL_DIR  = os.getenv("SSL_DIR",  os.path.join(_base, "ssl"))

# Let's Encrypt
LETSENCRYPT_DOMAIN  = os.getenv("LETSENCRYPT_DOMAIN", "")
LETSENCRYPT_EMAIL   = os.getenv("LETSENCRYPT_EMAIL", "")
LETSENCRYPT_STAGING = os.getenv("LETSENCRYPT_STAGING", "false").lower() == "true"

# Sessions
SESSION_COOKIE    = "session"
SESSION_MAX_AGE   = int(os.getenv("SESSION_MAX_AGE", str(60 * 60 * 24 * 30)))  # 30 days
SESSION_TOKEN_LEN = 48

# Uploads
UPLOAD_DIR      = db_config.cfg_get("upload_dir") or os.getenv("UPLOAD_DIR", "/var/www/appelo.nl/dev/communicatie/img")
UPLOAD_MAX_MB   = int(os.getenv("UPLOAD_MAX_MB", "10"))
ALLOWED_MIMES   = {"image/jpeg", "image/png", "image/gif", "image/webp"}
ALLOWED_MAGIC   = {
    b"\xff\xd8\xff": "image/jpeg",
    b"\x89PNG":      "image/png",
    b"GIF8":         "image/gif",
    b"RIFF":         "image/webp",  # checked further below
}

# Rate limiting (requests per window)
RATE_LIMIT_LOGIN    = int(os.getenv("RATE_LIMIT_LOGIN",    "10"))   # per minute per IP
RATE_LIMIT_MESSAGES = int(os.getenv("RATE_LIMIT_MESSAGES", "60"))   # per minute per user

# Federation
PEER_CONNECT_TIMEOUT = int(os.getenv("PEER_CONNECT_TIMEOUT", "10"))
