"""Runtime configuration, loaded from the environment (no hardcoded paths/ports)."""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8181
DEFAULT_DATABASE_URL = "postgresql://communicatie:communicatie@localhost/communicatie"


@dataclass(frozen=True)
class Config:
    """Server configuration."""

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    database_url: str = DEFAULT_DATABASE_URL

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Config:
        source = os.environ if env is None else env
        return cls(
            host=source.get("COMMUNICATIE_HOST", DEFAULT_HOST),
            port=int(source.get("COMMUNICATIE_PORT", str(DEFAULT_PORT))),
            database_url=source.get("COMMUNICATIE_DATABASE_URL", DEFAULT_DATABASE_URL),
        )
