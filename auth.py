import secrets
import uuid
from datetime import datetime, timezone, timedelta

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError, VerificationError, InvalidHashError

import config
import db

_ph = PasswordHasher()


def hash_password(password: str) -> str:
    return _ph.hash(password)


def verify_password(password: str, hashed: str) -> bool:
    try:
        return _ph.verify(hashed, password)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False


def needs_rehash(hashed: str) -> bool:
    return _ph.check_needs_rehash(hashed)


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

def _new_token() -> str:
    return secrets.token_urlsafe(config.SESSION_TOKEN_LEN)


def _expiry() -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=config.SESSION_MAX_AGE)


async def create_session(user_id: uuid.UUID) -> str:
    token = _new_token()
    await db.session_create(token, user_id, _expiry())
    return token


async def get_session(token: str | None) -> dict | None:
    if not token:
        return None
    return await db.session_get(token)


async def delete_session(token: str):
    await db.session_delete(token)


# ---------------------------------------------------------------------------
# aiohttp request helper
# ---------------------------------------------------------------------------

async def session_from_request(request) -> dict | None:
    token = request.cookies.get(config.SESSION_COOKIE)
    return await get_session(token)


def set_session_cookie(response, token: str):
    response.set_cookie(
        config.SESSION_COOKIE,
        token,
        max_age=config.SESSION_MAX_AGE,
        httponly=True,
        secure=True,
        samesite="Strict",
    )


def clear_session_cookie(response):
    response.del_cookie(config.SESSION_COOKIE, samesite="Strict")
