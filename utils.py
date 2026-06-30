import json
import uuid
from datetime import datetime


class _Encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, uuid.UUID):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


def _dumps(obj: dict) -> str:
    return json.dumps(obj, cls=_Encoder)


def _ok(type_: str, **data) -> dict:
    return {"type": type_, "ok": True, **data}


def _err(type_: str, reason: str, **extra) -> dict:
    return {"type": type_, "ok": False, "reason": reason, **extra}


def _str_uuid(val) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(val))
    except (ValueError, AttributeError):
        return None
