import os

# Database
DB_DSN = os.getenv("DB_DSN", "postgresql://communicatie:communicatie@localhost/communicatie")
# Note: default password is 'communicatie' (double-m)

# Server
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "443"))
PORT_HTTP = int(os.getenv("PORT_HTTP", "80"))

# SSL
_base    = os.path.dirname(os.path.abspath(__file__))
SSL_CERT = os.getenv("SSL_CERT", os.path.join(_base, "ssl", "cert.pem"))
SSL_KEY  = os.getenv("SSL_KEY",  os.path.join(_base, "ssl", "key.pem"))
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
UPLOAD_DIR      = os.getenv("UPLOAD_DIR", "/var/www/appelo.nl/dev/communicatie/img")
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
