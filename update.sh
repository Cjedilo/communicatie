#!/usr/bin/env bash
# Messages — apply a new release
# Run manually or via the systemd timer installed by install.sh.
# Exits 0 always so the timer doesn't report a failure when nothing changed.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_NAME="messages"
CONFIG_DB="$REPO_DIR/_config.db"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[update]${NC} $*"; }
warn() { echo -e "${YELLOW}[update]${NC} $*"; }

# ── 1. Check whether auto-update is enabled ──────────────────────────
ENABLED=$(python3 -c "
import sqlite3
try:
    db = sqlite3.connect('$CONFIG_DB')
    row = db.execute(\"SELECT value FROM config WHERE key='auto_update'\").fetchone()
    print(row[0] if row else '0')
except Exception:
    print('0')
" 2>/dev/null || echo "0")

if [ "$ENABLED" != "1" ]; then
    warn "Auto-update is disabled. Enable it in Advanced settings or re-run install.sh."
    exit 0
fi

log "Messages update check — $(date)"

# ── 2. Fetch latest tags from remote ─────────────────────────────────
git -C "$REPO_DIR" fetch --tags --quiet || {
    warn "Could not reach remote repository — skipping update."
    exit 0
}

# ── 3. Determine current and latest version ──────────────────────────
CURRENT=$(git -C "$REPO_DIR" describe --tags --exact-match 2>/dev/null \
       || git -C "$REPO_DIR" describe --tags 2>/dev/null \
       || git -C "$REPO_DIR" rev-parse --short HEAD 2>/dev/null \
       || echo "unknown")

LATEST=$(git -C "$REPO_DIR" tag --sort=-version:refname 2>/dev/null | head -1)

if [ -z "$LATEST" ]; then
    warn "No release tags found in the repository — nothing to update."
    exit 0
fi

# Check if HEAD is already at or ahead of the latest tag
BEHIND=$(git -C "$REPO_DIR" rev-list --count "HEAD..refs/tags/$LATEST" 2>/dev/null || echo "0")
if [ "$BEHIND" = "0" ]; then
    log "Already at latest release ($CURRENT). Nothing to do."
    exit 0
fi

log "Update available: $CURRENT → $LATEST"
log "Applying..."

# ── 4. Checkout the new release ───────────────────────────────────────
git -C "$REPO_DIR" checkout "$LATEST" --quiet

# ── 5. Update Python dependencies ────────────────────────────────────
if [ -f "$REPO_DIR/venv/bin/pip" ]; then
    "$REPO_DIR/venv/bin/pip" install --quiet -r "$REPO_DIR/requirements.txt"
else
    warn "venv not found — skipping pip install."
fi

# ── 6. Restart the service ────────────────────────────────────────────
if command -v systemctl &>/dev/null; then
    log "Restarting $SERVICE_NAME service..."
    systemctl restart "$SERVICE_NAME" 2>/dev/null \
        || sudo systemctl restart "$SERVICE_NAME" 2>/dev/null \
        || warn "Could not restart service — restart manually."
else
    warn "systemctl not found. Restart manually: $REPO_DIR/venv/bin/python $REPO_DIR/main.py"
fi

log "Updated Messages to $LATEST"
