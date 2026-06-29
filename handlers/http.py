import io
import logging
import os
import secrets
import uuid
from pathlib import Path

import aiohttp_jinja2
from aiohttp import web
from PIL import Image, ImageOps

import auth
import config
import db
import db_config

log = logging.getLogger(__name__)

UPLOAD_MAX_BYTES = config.UPLOAD_MAX_MB * 1024 * 1024

# Magic bytes → mime type (first 4 bytes sufficient for our allowed types)
MAGIC = [
    (b"\xff\xd8\xff",  "image/jpeg"),
    (b"\x89PNG",       "image/png"),
    (b"GIF8",          "image/gif"),
    (b"RIFF",          "image/webp"),
]

EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png":  ".png",
    "image/gif":  ".gif",
    "image/webp": ".webp",
}


AVATAR_SIZE  = (32, 32)
IMAGE_MAX_PX = 1920


def _detect_mime(data: bytes) -> str | None:
    for magic, mime in MAGIC:
        if data[:len(magic)] == magic:
            if mime == "image/webp" and data[8:12] != b"WEBP":
                continue
            return mime
    return None


def _to_rgb(img: Image.Image) -> Image.Image:
    if img.mode in ("RGBA", "LA", "P"):
        background = Image.new("RGB", img.size, (255, 255, 255))
        background.paste(img, mask=img.convert("RGBA").split()[3])
        return background
    return img.convert("RGB") if img.mode != "RGB" else img


def _resize_avatar(data: bytes) -> bytes:
    img = Image.open(io.BytesIO(data))
    img = ImageOps.exif_transpose(img)
    img = _to_rgb(img)
    img = ImageOps.fit(img, AVATAR_SIZE, Image.LANCZOS)
    out = io.BytesIO()
    img.save(out, format="JPEG", quality=85, optimize=True)
    return out.getvalue()


def _resize_image(data: bytes) -> bytes:
    img = Image.open(io.BytesIO(data))

    # Animated GIF: re-encoding frame-by-frame is lossy and fiddly (palette,
    # transparency, per-frame timing, disposal), so keep every frame by
    # returning the original bytes unchanged. Size is already capped on upload.
    if getattr(img, "is_animated", False):
        return data

    if img.format == "GIF":
        # Static GIF — keep as GIF to preserve transparency, just cap dimensions.
        if img.width > IMAGE_MAX_PX or img.height > IMAGE_MAX_PX:
            img.thumbnail((IMAGE_MAX_PX, IMAGE_MAX_PX), Image.LANCZOS)
        out = io.BytesIO()
        img.save(out, format="GIF")
        return out.getvalue()

    # Other still images → normalise to JPEG, capped.
    img = ImageOps.exif_transpose(img)
    img = _to_rgb(img)
    if img.width > IMAGE_MAX_PX or img.height > IMAGE_MAX_PX:
        img.thumbnail((IMAGE_MAX_PX, IMAGE_MAX_PX), Image.LANCZOS)
    out = io.BytesIO()
    img.save(out, format="JPEG", quality=85, optimize=True)
    return out.getvalue()


async def _save_upload(field, *, avatar: bool = False) -> str:
    data = await field.read(decode=False)
    if len(data) > UPLOAD_MAX_BYTES:
        raise web.HTTPRequestEntityTooLarge(max_size=UPLOAD_MAX_BYTES, actual_size=len(data))

    mime = _detect_mime(data)
    if mime not in config.ALLOWED_MIMES:
        raise web.HTTPUnsupportedMediaType(reason="Only JPEG, PNG, GIF, WEBP allowed")

    try:
        if avatar:
            data = _resize_avatar(data)
            ext  = ".jpg"
        else:
            data = _resize_image(data)
            ext  = ".gif" if data[:6] in (b"GIF87a", b"GIF89a") else ".jpg"
    except Exception:
        raise web.HTTPBadRequest(reason="Could not process image")

    filename = f"{uuid.uuid4().hex}{ext}"
    dest = Path(config.UPLOAD_DIR) / filename
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return filename


# ---------------------------------------------------------------------------
# Page routes — serve Jinja2 templates
# ---------------------------------------------------------------------------

def _tpl_ctx(session, owner_id) -> dict:
    return {
        "logged_in":  session is not None,
        "user_name":  session["name"] if session else "",
        "user_id":    str(session["user_id"]) if session else "",
        "is_owner":   session and str(session["user_id"]) == owner_id,
        "base_path":  config.BASE_PATH,
    }


async def index(request: web.Request) -> web.Response:
    session  = await auth.session_from_request(request)
    owner_id = await db.setting_get("owner_id")
    return aiohttp_jinja2.render_template("index.html", request, _tpl_ctx(session, owner_id))


_TEMPLATES_DIR = Path(__file__).parent.parent / "templates"

def raw_file(filename: str, content_type: str):
    path = _TEMPLATES_DIR / filename
    body = path.read_bytes()
    async def _handler(request: web.Request) -> web.Response:
        return web.Response(body=body, content_type=content_type)
    return _handler


def page(template: str, content_type: str = "text/html"):
    async def _handler(request: web.Request) -> web.Response:
        session  = await auth.session_from_request(request)
        owner_id = await db.setting_get("owner_id")
        response = aiohttp_jinja2.render_template(template, request, _tpl_ctx(session, owner_id))
        response.content_type = content_type
        return response
    return _handler


def wiki_page(slug: str):
    """Serve a wiki page — no auth required, publicly readable."""
    template = f"wiki/{slug}.html"
    async def _handler(request: web.Request) -> web.Response:
        ctx = {"base_path": config.BASE_PATH}
        try:
            return aiohttp_jinja2.render_template(template, request, ctx)
        except Exception:
            raise web.HTTPNotFound()
    return _handler


# ---------------------------------------------------------------------------
# Upload endpoints
# ---------------------------------------------------------------------------

async def set_avatar(request: web.Request) -> web.Response:
    session = await auth.session_from_request(request)
    if not session:
        raise web.HTTPUnauthorized()

    reader = await request.multipart()
    field = await reader.next()
    if not field or field.name != "avatar":
        raise web.HTTPBadRequest(reason="Missing avatar field")

    filename = await _save_upload(field, avatar=True)
    await db.user_set_avatar(session["user_id"], filename)
    return web.json_response({"avatar": filename})


async def upload_image(request: web.Request) -> web.Response:
    session = await auth.session_from_request(request)
    if not session:
        raise web.HTTPUnauthorized()

    reader = await request.multipart()
    field = await reader.next()
    if not field or field.name != "image":
        raise web.HTTPBadRequest(reason="Missing image field")

    filename = await _save_upload(field)
    return web.json_response({"image": filename})


# ---------------------------------------------------------------------------
# Auth endpoints
# ---------------------------------------------------------------------------

def _client_ip(request: web.Request) -> str:
    """Real client IP for rate limiting. X-Forwarded-For is attacker-controlled
    unless the direct peer is a trusted reverse proxy; only then do we read the
    rightmost forwarded entry (the address our proxy actually observed)."""
    remote = request.remote or ""
    if remote in config.TRUSTED_PROXIES:
        xff = request.headers.get("X-Forwarded-For", "")
        if xff:
            return xff.split(",")[-1].strip()
    return remote


# key -> (window_start_monotonic, count_in_window)
_auth_attempts: dict[str, tuple[float, int]] = {}
_AUTH_WINDOW = 60.0


def _auth_rate_ok(key: str) -> bool:
    import time
    now = time.monotonic()
    start, count = _auth_attempts.get(key, (now, 0))
    if now - start > _AUTH_WINDOW:        # fixed window — reset, don't slide
        start, count = now, 0
    count += 1
    _auth_attempts[key] = (start, count)
    # Bound memory: evict stale windows once the table grows large.
    if len(_auth_attempts) > 10000:
        cutoff = now - _AUTH_WINDOW
        for k in [k for k, (s, _) in _auth_attempts.items() if s < cutoff]:
            del _auth_attempts[k]
    return count <= config.RATE_LIMIT_LOGIN


async def register(request: web.Request) -> web.Response:
    if not _auth_rate_ok(_client_ip(request)):
        return web.json_response({"ok": False, "reason": "Too many attempts, try again later"}, status=429)

    try:
        data = await request.json()
    except Exception:
        raise web.HTTPBadRequest()

    name     = str(data.get("name", "")).strip()[:64]
    password = str(data.get("password", ""))

    if not name or not password:
        return web.json_response({"ok": False, "reason": "Name and password required"})
    if len(password) < 8:
        return web.json_response({"ok": False, "reason": "Password must be at least 8 characters"})
    if await db.user_by_name(name):
        return web.json_response({"ok": False, "reason": "Name already taken"})

    hashed = auth.hash_password(password)
    user   = await db.user_create(name, hashed)

    if not await db.setting_get("owner_id"):
        await db.setting_set("owner_id", str(user["id"]))

    return web.json_response({"ok": True})


async def login(request: web.Request) -> web.Response:
    if not _auth_rate_ok(_client_ip(request)):
        return web.json_response({"ok": False, "reason": "Too many attempts, try again later"}, status=429)

    try:
        data = await request.json()
    except Exception:
        raise web.HTTPBadRequest()

    name     = str(data.get("name", "")).strip()[:64]
    password = str(data.get("password", ""))

    if not name or not password:
        return web.json_response({"ok": False, "reason": "Name and password required"})

    user = await db.user_by_name(name)
    if not user or not auth.verify_password(password, user["password"]):
        return web.json_response({"ok": False, "reason": "Invalid name or password"})

    # Rehash if argon2 params have been upgraded
    if auth.needs_rehash(user["password"]):
        await db.user_set_password(user["id"], auth.hash_password(password))

    token    = await auth.create_session(user["id"])
    response = web.json_response({"ok": True})
    auth.set_session_cookie(response, token)
    return response


async def setup_claim(request: web.Request) -> web.Response:
    """One-time owner-claim endpoint. Secret embedded in URL; invalidated after use."""
    secret = request.match_info.get("secret", "")
    stored = db_config.setup_secret_get()

    if not stored or not secrets.compare_digest(stored, secret):
        raise web.HTTPNotFound()

    if await db.setting_get("owner_id"):
        # Already claimed — secret should be gone, but be safe
        db_config.setup_secret_clear()
        raise web.HTTPNotFound()

    if request.method == "GET":
        ctx = {"base_path": config.BASE_PATH}
        return aiohttp_jinja2.render_template("setup.html", request, ctx)

    # POST — register the owner account
    try:
        data = await request.json()
    except Exception:
        raise web.HTTPBadRequest()

    name     = str(data.get("name", "")).strip()[:64]
    password = str(data.get("password", ""))

    if not name or not password:
        return web.json_response({"ok": False, "reason": "Name and password required"})
    if len(password) < 8:
        return web.json_response({"ok": False, "reason": "Password must be at least 8 characters"})
    if await db.user_by_name(name):
        return web.json_response({"ok": False, "reason": "Name already taken"})

    hashed = auth.hash_password(password)
    user   = await db.user_create(name, hashed)
    await db.setting_set("owner_id", str(user["id"]))

    db_config.setup_secret_clear()

    token    = await auth.create_session(user["id"])
    response = web.json_response({"ok": True})
    auth.set_session_cookie(response, token)
    return response


async def proxy_img(request: web.Request) -> web.Response:
    """Proxy a remote peer's image through the local server to avoid cert trust issues."""
    session = await auth.session_from_request(request)
    if not session:
        raise web.HTTPUnauthorized()

    url = request.query.get("url", "")
    if not url.startswith("https://") and not url.startswith("http://"):
        raise web.HTTPBadRequest(reason="Invalid URL")

    # SSRF guard: this proxy exists only to fetch images from peers whose
    # self-signed certs the browser won't trust. Restrict the target host to a
    # known peer's address — otherwise an authenticated user (or a malicious
    # peer-supplied avatar URL) could make us fetch internal/metadata endpoints.
    import urllib.parse
    target_host = urllib.parse.urlparse(url).netloc.lower()
    allowed_hosts = {
        urllib.parse.urlparse(p["address"]).netloc.lower()
        for p in await db.peers_all()
    }
    allowed_hosts.discard("")
    if target_host not in allowed_hosts:
        raise web.HTTPForbidden(reason="URL host is not a known peer")

    import aiohttp as _aiohttp, ssl as _ssl
    ctx = _ssl.SSLContext(_ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = _ssl.CERT_NONE
    try:
        async with _aiohttp.ClientSession() as s:
            async with s.get(url, ssl=ctx, timeout=_aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    raise web.HTTPNotFound()
                data         = await resp.read()
                content_type = resp.content_type or "image/jpeg"
        return web.Response(body=data, content_type=content_type,
                            headers={"Cache-Control": "max-age=86400"})
    except web.HTTPException:
        raise
    except Exception:
        raise web.HTTPBadGateway()


async def succession(request: web.Request) -> web.Response:
    """Public endpoint — no auth. Peers fetch this to resolve cert rotations."""
    return web.json_response({"records": db_config.succession_all()})


async def logout(request: web.Request) -> web.Response:
    token = request.cookies.get(config.SESSION_COOKIE)
    if token:
        await auth.delete_session(token)
    response = web.HTTPFound((config.BASE_PATH or "") + "/")
    auth.clear_session_cookie(response)
    return response


# ---------------------------------------------------------------------------
# Route table
# ---------------------------------------------------------------------------

routes = [
    ("GET",  "/",              index),
    ("GET",  "/login.html",    page("login.html")),
    ("GET",  "/chat.html",     page("chat.html")),
    ("GET",  "/channels.html", page("channels.html")),
    ("GET",  "/user.html",     page("user.html")),
    ("GET",  "/stream.html",   page("stream.html")),
    ("GET",  "/peers.html",    page("peers.html")),
    ("GET",  "/server.html",    page("server.html")),
    ("GET",  "/advanced.html", page("advanced.html")),
    ("GET",  "/users.html",    page("users.html")),
    ("GET",  "/setup/{secret}",              setup_claim),
    ("POST", "/setup/{secret}",              setup_claim),
    ("GET",  "/wiki/",                       wiki_page("home")),
    ("GET",  "/wiki/getting-started",        wiki_page("getting-started")),
    ("GET",  "/wiki/channels",               wiki_page("channels")),
    ("GET",  "/wiki/users",                   wiki_page("users-page")),
    ("GET",  "/wiki/peers",                  wiki_page("peers")),
    ("GET",  "/wiki/server",                 wiki_page("server")),
    ("GET",  "/wiki/security",               wiki_page("security")),
    ("GET",  "/wiki/federation-protocol",    wiki_page("federation-protocol")),
    ("GET",  "/wiki/privacy",                wiki_page("privacy")),
    ("GET",  "/wiki.css",                    raw_file("wiki.css", "text/css")),
    ("GET",  "/communicatie.css", page("communicatie.css", "text/css")),
    ("GET",  "/communicatie.js",  page("communicatie.js",  "application/javascript")),
    ("GET",  "/emoji-picker.js",   raw_file("emoji-picker.js",   "application/javascript")),
    ("GET",  "/emoji-data.json",   raw_file("emoji-data.json",   "application/json")),
    ("POST", "/register",      register),
    ("POST", "/login",         login),
    ("POST", "/set_avatar",    set_avatar),
    ("POST", "/upload",        upload_image),
    ("GET",  "/proxy_img",     proxy_img),
    ("GET",  "/succession",    succession),
    ("GET",  "/logout",        logout),
]
