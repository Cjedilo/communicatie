# communicatie

Federated chat where **every user runs their own server and owns their data**. Multiple servers
share the same chat, and each user's messages are always served from that user's own server.

This branch (`full-rewrite`) is a clean rewrite. The original code lives in the git history on
`master` and served as inspiration. See `REWRITE_PLAN.md` (not committed) for the full design and
step-by-step plan.

## Design decisions

- **Storage:** PostgreSQL.
- **Trust between servers:** keypairs + signed messages (trustless federation).
- **Web framework:** aiohttp (browser and peer transport both over WebSockets).
- **User identity:** a user *is* their public key (global id across servers).
- **Peer transport:** a single bidirectional WebSocket per peer pair.
- **Frontend:** plain vanilla JS, no build step.

## Layout

```
src/communicatie/    # the package
tests/               # pytest tests
```

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

ruff check .         # lint
ruff format --check .  # formatting
pytest               # tests
```

Configuration comes from the environment (see `.env.example`); there are no hardcoded paths or
ports.
