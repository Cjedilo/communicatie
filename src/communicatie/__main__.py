"""Entry point. For now it only loads config; the server arrives in fase 2."""

from __future__ import annotations

import logging

from communicatie.config import Config


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    config = Config.from_env()
    logging.info("communicatie config loaded: host=%s port=%s", config.host, config.port)


if __name__ == "__main__":
    main()
