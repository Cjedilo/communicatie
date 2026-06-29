#!/usr/bin/env bash
# Messages — installer for Linux
# Usage: ./install.sh
set -euo pipefail

# ── Colours ───────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; BOLD='\033[1m'; NC='\033[0m'
step()  { echo -e "\n${GREEN}==>${NC} ${BOLD}$*${NC}"; }
warn()  { echo -e "${YELLOW}  ⚠ $*${NC}"; }
err()   { echo -e "${RED}  ✗ $*${NC}"; exit 1; }
ok()    { echo -e "${GREEN}  ✓ $*${NC}"; }

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="messages"
CONFIG_DB="$REPO_DIR/_config.db"

echo ""
echo -e "${BOLD}Messages — self-hosted federated chat${NC}"
echo "────────────────────────────────────────"

# ── 1. Python version ─────────────────────────────────────────────
step "Checking Python version"
if ! command -v python3 &>/dev/null; then
    err "python3 not found. Install Python 3.12 or newer and re-run."
fi
PY_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PY_MAJOR=$(echo "$PY_VER" | cut -d. -f1)
PY_MINOR=$(echo "$PY_VER" | cut -d. -f2)
if [ "$PY_MAJOR" -lt 3 ] || { [ "$PY_MAJOR" -eq 3 ] && [ "$PY_MINOR" -lt 12 ]; }; then
    err "Python 3.12+ required (found $PY_VER)."
fi
ok "Python $PY_VER"

# ── 2. Virtual environment + dependencies ─────────────────────────
step "Installing dependencies"
python3 -m venv "$REPO_DIR/venv"
"$REPO_DIR/venv/bin/pip" install --quiet --upgrade pip
"$REPO_DIR/venv/bin/pip" install --quiet -r "$REPO_DIR/requirements.txt"
ok "Dependencies installed"

# ── Helper: check if a TCP port is free ──────────────────────────
port_free() {
    ! (ss -tlnp 2>/dev/null | grep -q ":$1 " \
    || netstat -tlnp 2>/dev/null | grep -q ":$1 " \
    || lsof -iTCP:"$1" -sTCP:LISTEN &>/dev/null 2>&1)
}

ask_port() {
    local label="$1" default="$2" var="$3"
    if port_free "$default"; then
        eval "$var=$default"
    else
        warn "Port $default is already in use."
        while true; do
            read -rp "  Enter $label port [$3 suggested: 0 to disable]: " input
            input="${input:-}"
            [ -z "$input" ] && { eval "$var=0"; break; }
            if [[ "$input" =~ ^[0-9]+$ ]] && [ "$input" -le 65535 ]; then
                if [ "$input" -eq 0 ] || port_free "$input"; then
                    eval "$var=$input"; break
                else
                    warn "Port $input is also in use. Try another."
                fi
            else
                warn "Invalid port number."
            fi
        done
    fi
}

# ── 3. Ports ──────────────────────────────────────────────────────
step "Configuring ports"

PORT=443
if ! port_free 443; then
    warn "Port 443 is already in use."
    while true; do
        read -rp "  Enter HTTPS port [8443]: " PORT
        PORT="${PORT:-8443}"
        if [[ "$PORT" =~ ^[0-9]+$ ]] && [ "$PORT" -le 65535 ]; then
            if port_free "$PORT"; then break; else warn "Port $PORT is also in use."; fi
        else
            warn "Invalid port."
        fi
    done
fi
ok "HTTPS port: $PORT"

PORT_HTTP=80
if ! port_free 80; then
    warn "Port 80 is already in use."
    read -rp "  Enter HTTP redirect port (0 to disable) [0]: " PORT_HTTP
    PORT_HTTP="${PORT_HTTP:-0}"
fi
ok "HTTP redirect port: ${PORT_HTTP} ($([ "$PORT_HTTP" -eq 0 ] && echo disabled || echo enabled))"

# ── 4. Upload directory ───────────────────────────────────────────
step "Configuring storage"
DEFAULT_UPLOAD="/var/lib/$SERVICE_NAME/uploads"
while true; do
    read -rp "  Upload directory [$DEFAULT_UPLOAD]: " UPLOAD_DIR
    UPLOAD_DIR="${UPLOAD_DIR:-$DEFAULT_UPLOAD}"
    if mkdir -p "$UPLOAD_DIR" 2>/dev/null && touch "$UPLOAD_DIR/.write_test" 2>/dev/null; then
        rm -f "$UPLOAD_DIR/.write_test"
        ok "Upload directory: $UPLOAD_DIR"
        break
    else
        warn "Cannot write to '$UPLOAD_DIR'. Try a different path."
    fi
done

# ── 5. Generate setup secret + write _config.db ───────────────────
step "Generating setup secret"
SETUP_SECRET=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
python3 - <<PYEOF
import sqlite3
db = sqlite3.connect("$CONFIG_DB")
db.execute("CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY, value TEXT)")
for k, v in [
    ("port",         "$PORT"),
    ("port_http",    "$PORT_HTTP"),
    ("upload_dir",   "$UPLOAD_DIR"),
    ("setup_secret", "$SETUP_SECRET"),
]:
    db.execute("INSERT OR REPLACE INTO config(key,value) VALUES(?,?)", (k, v))
db.commit()
db.close()
PYEOF
ok "Config stored in _config.db"

# ── 6. Detect local IP ────────────────────────────────────────────
DETECTED_IP=$(python3 -c "
import socket
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    print(s.getsockname()[0])
    s.close()
except Exception:
    print('localhost')
")

# ── 7. Systemd service ────────────────────────────────────────────
step "Installing systemd service"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
cat > /tmp/${SERVICE_NAME}.service <<EOF
[Unit]
Description=Messages — self-hosted federated chat
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$REPO_DIR
ExecStart=$REPO_DIR/venv/bin/python main.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

if [ "$EUID" -eq 0 ]; then
    mv /tmp/${SERVICE_NAME}.service "$SERVICE_FILE"
else
    sudo mv /tmp/${SERVICE_NAME}.service "$SERVICE_FILE" \
        || { warn "Could not install systemd service (no sudo). Run as root or install manually."; }
fi

if command -v systemctl &>/dev/null; then
    sudo systemctl daemon-reload              2>/dev/null || true
    sudo systemctl enable "$SERVICE_NAME"    2>/dev/null || true
    sudo systemctl restart "$SERVICE_NAME"   2>/dev/null || true
    ok "Service '$SERVICE_NAME' enabled and started"
else
    warn "systemctl not found — start manually: $REPO_DIR/venv/bin/python $REPO_DIR/main.py"
fi

# ── 8. Auto-update (optional) ─────────────────────────────────────────
step "Automatic updates"
echo "   Messages can automatically install new tagged releases every night."
echo "   Only official releases (git tags) are installed — never untagged commits."
read -rp "  Enable automatic updates? [y/N]: " AUTOUPDATE_CHOICE
AUTOUPDATE_CHOICE="${AUTOUPDATE_CHOICE:-n}"

if [[ "$AUTOUPDATE_CHOICE" =~ ^[Yy]$ ]]; then
    # Store setting in config
    python3 -c "
import sqlite3
db = sqlite3.connect('$CONFIG_DB')
db.execute(\"INSERT OR REPLACE INTO config(key,value) VALUES('auto_update','1')\")
db.commit()
db.close()
"
    # Create update service unit
    cat > /tmp/${SERVICE_NAME}-update.service <<EOF
[Unit]
Description=Messages auto-update
After=network.target

[Service]
Type=oneshot
User=$USER
WorkingDirectory=$REPO_DIR
ExecStart=$REPO_DIR/update.sh
StandardOutput=journal
StandardError=journal
EOF

    # Create update timer unit (nightly 03:00, ±30 min random jitter)
    cat > /tmp/${SERVICE_NAME}-update.timer <<EOF
[Unit]
Description=Messages nightly update check

[Timer]
OnCalendar=*-*-* 03:00:00
RandomizedDelaySec=1800
Persistent=true

[Install]
WantedBy=timers.target
EOF

    if [ "$EUID" -eq 0 ]; then
        mv /tmp/${SERVICE_NAME}-update.service "/etc/systemd/system/${SERVICE_NAME}-update.service"
        mv /tmp/${SERVICE_NAME}-update.timer   "/etc/systemd/system/${SERVICE_NAME}-update.timer"
    else
        sudo mv /tmp/${SERVICE_NAME}-update.service "/etc/systemd/system/${SERVICE_NAME}-update.service" 2>/dev/null || true
        sudo mv /tmp/${SERVICE_NAME}-update.timer   "/etc/systemd/system/${SERVICE_NAME}-update.timer"   2>/dev/null || true
    fi

    if command -v systemctl &>/dev/null; then
        sudo systemctl daemon-reload                          2>/dev/null || true
        sudo systemctl enable "${SERVICE_NAME}-update.timer" 2>/dev/null || true
        sudo systemctl start  "${SERVICE_NAME}-update.timer" 2>/dev/null || true
        ok "Auto-update timer installed (runs nightly at 03:00)"
    else
        warn "systemctl not found — timer could not be registered."
    fi
else
    python3 -c "
import sqlite3
db = sqlite3.connect('$CONFIG_DB')
db.execute(\"INSERT OR REPLACE INTO config(key,value) VALUES('auto_update','0')\")
db.commit()
db.close()
"
    ok "Auto-update disabled (can be enabled later in Advanced settings)"
fi

# ── 9. Build setup URL ────────────────────────────────────────────
if [ "$PORT" -eq 443 ]; then
    SETUP_URL="https://${DETECTED_IP}/setup/${SETUP_SECRET}"
else
    SETUP_URL="https://${DETECTED_IP}:${PORT}/setup/${SETUP_SECRET}"
fi

# ── Done ──────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
echo -e "${BOLD} Messages installed successfully!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e " Open this URL to claim your server as owner:"
echo ""
echo -e "   ${BOLD}${SETUP_URL}${NC}"
echo ""
echo -e " ${YELLOW}Keep this URL safe — it works once.${NC}"
echo -e " The URL is also logged each time the server starts"
echo -e " until the server has been claimed."
echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
