import io
import logging
import os
import uuid
from pathlib import Path

import aiohttp_jinja2
from aiohttp import web
from PIL import Image, ImageOps

import auth
import config
import db

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
    is_gif = getattr(img, "is_animated", False) or img.format == "GIF"
    if not is_gif:
        img = ImageOps.exif_transpose(img)
        img = _to_rgb(img)
        if img.width > IMAGE_MAX_PX or img.height > IMAGE_MAX_PX:
            img.thumbnail((IMAGE_MAX_PX, IMAGE_MAX_PX), Image.LANCZOS)
        out = io.BytesIO()
        img.save(out, format="JPEG", quality=85, optimize=True)
    else:
        # Keep GIF as-is, just cap dimensions
        if img.width > IMAGE_MAX_PX or img.height > IMAGE_MAX_PX:
            img.thumbnail((IMAGE_MAX_PX, IMAGE_MAX_PX), Image.LANCZOS)
        out = io.BytesIO()
        img.save(out, format="GIF")
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

async def index(request: web.Request) -> web.Response:
    session = await auth.session_from_request(request)
    owner_id = await db.setting_get("owner_id")
    return aiohttp_jinja2.render_template(
        "index.html", request,
        {
            "logged_in": session is not None,
            "user_name": session["name"] if session else "",
            "user_id":   str(session["user_id"]) if session else "",
            "is_owner":  session and str(session["user_id"]) == owner_id,
        }
    )


def page(template: str, content_type: str = "text/html"):
    async def _handler(request: web.Request) -> web.Response:
        session = await auth.session_from_request(request)
        owner_id = await db.setting_get("owner_id")
        response = aiohttp_jinja2.render_template(
            template, request,
            {
                "logged_in": session is not None,
                "user_name": session["name"] if session else "",
                "user_id":   str(session["user_id"]) if session else "",
                "is_owner":  session and str(session["user_id"]) == owner_id,
            }
        )
        response.content_type = content_type
        return response
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

_register_attempts: dict[str, tuple[float, int]] = {}


def _register_rate_ok(ip: str) -> bool:
    import time
    now   = time.monotonic()
    last, count = _register_attempts.get(ip, (now, 0))
    if now - last > 60:
        count = 0
    count += 1
    _register_attempts[ip] = (now, count)
    return count <= config.RATE_LIMIT_LOGIN


async def register(request: web.Request) -> web.Response:
    ip = request.headers.get("X-Forwarded-For", request.remote)
    if not _register_rate_ok(ip):
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
    ip = request.headers.get("X-Forwarded-For", request.remote)
    if not _register_rate_ok(ip):
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


async def proxy_img(request: web.Request) -> web.Response:
    """Proxy a remote peer's image through the local server to avoid cert trust issues."""
    session = await auth.session_from_request(request)
    if not session:
        raise web.HTTPUnauthorized()

    url = request.query.get("url", "")
    if not url.startswith("https://") and not url.startswith("http://"):
        raise web.HTTPBadRequest(reason="Invalid URL")

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


async def logout(request: web.Request) -> web.Response:
    token = request.cookies.get(config.SESSION_COOKIE)
    if token:
        await auth.delete_session(token)
    response = web.HTTPFound("/")
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
    ("GET",  "/communicatie.css", page("communicatie.css", "text/css")),
    ("GET",  "/communicatie.js",  page("communicatie.js",  "application/javascript")),
    ("POST", "/register",      register),
    ("POST", "/login",         login),
    ("POST", "/set_avatar",    set_avatar),
    ("POST", "/upload",        upload_image),
    ("GET",  "/proxy_img",     proxy_img),
    ("GET",  "/logout",        logout),
]
