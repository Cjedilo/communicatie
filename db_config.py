"""
Persistent config store — always SQLite.

When DB_DSN env var points to a SQLite file, that file is used for config too
(so each instance is fully self-contained in its own DB).
Otherwise falls back to _config.db next to the source files.
"""
import json
import os
import sqlite3
from pathlib import Path

_env_dsn = os.getenv("DB_DSN", "")
if _env_dsn.startswith("sqlite://"):
    _PATH = Path(_env_dsn.removeprefix("sqlite://"))
else:
    _PATH = Path(__file__).parent / "_config.db"


def _open():
    c = sqlite3.connect(_PATH)
    c.execute("CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY, value TEXT)")
    c.execute("""CREATE TABLE IF NOT EXISTS succession(
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        old_fp  TEXT NOT NULL UNIQUE,
        record  TEXT NOT NULL
    )""")
    return c


def cfg_get(key: str) -> str | None:
    try:
        with _open() as c:
            r = c.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
            return r[0] if r else None
    except Exception:
        return None


def cfg_set(key: str, value: str) -> None:
    with _open() as c:
        c.execute(
            "INSERT INTO config(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
            (key, value),
        )


def succession_add(record: dict) -> None:
    with _open() as c:
        c.execute(
            "INSERT OR REPLACE INTO succession(old_fp, record) VALUES(?,?)",
            (record["old_fingerprint"], json.dumps(record)),
        )


def succession_all() -> list[dict]:
    try:
        with _open() as c:
            rows = c.execute("SELECT record FROM succession ORDER BY id").fetchall()
            return [json.loads(r[0]) for r in rows]
    except Exception:
        return []
