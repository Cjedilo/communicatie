# Messages

Self-hosted, federated chat. Your data, your server, your people.

Run your own server on hardware you control. Connect it to friends' and
family's servers to chat across them — like email, but for group chat. No
central company sits between you and your messages.

## Features

- **Channels** — public and private, with icons, bans, and a live stream view
- **Federation** — connect to other Messages servers; chat across servers
  without anyone's content leaving the server it was posted on
- **Direct messages & scratchpad** — DMs with local or remote users, plus a
  private notes channel just for you
- **Self-contained** — SQLite or PostgreSQL, self-signed or your own TLS
  certificate, optional Let's Encrypt
- **Built-in wiki** — every feature documented in the app itself at `/wiki/`
- **One-command install** — `install.sh` / `install.cmd` set up a venv,
  systemd service (or Windows Task Scheduler), and a one-time claim URL
- **Optional auto-update** — nightly check for new tagged releases, opt-in

See the [wiki](templates/wiki/) (or `/wiki/` on a running instance) for the
full picture: getting started, the security model, and how federation works
under the hood.

## Requirements

- Python 3.12+
- Linux or Windows
- SQLite (default) or PostgreSQL

## Install

```bash
git clone https://github.com/Cjedilo/communicatie.git
cd communicatie
./install.sh        # Windows: install.cmd
```

The installer asks a few questions — which ports to use, where to store
uploaded images, whether to enable auto-updates — sets up Messages as a
background service, and prints a one-time URL. Open it to claim the server
as owner:

```
Open this URL to claim your server as owner:

  https://192.168.1.10:8443/setup/a3f9c2...
```

That's it — Messages is running. Full walkthrough, including port
forwarding, is in the in-app wiki under **Getting started**.

## Manual run (development)

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/python main.py
```

Configuration is read from `_config.db` (set via the Advanced settings page
once running) and falls back to environment variables — see `config.py` for
the full list (`HOST`, `PORT`, `DB_DSN`, `UPLOAD_DIR`, rate limits, session
duration, and more).

## Updating

```bash
./update.sh          # Windows: update.ps1
```

Only applies tagged releases, never untagged commits on `main`. Auto-update
can also be toggled from Advanced settings without re-running the installer.

## License

MIT
