# Messages — apply a new release (Windows)
# Run manually or via the Task Scheduler task installed by install.ps1.

$ErrorActionPreference = "Stop"
$RepoDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ServiceName = "Messages"
$ConfigDb = Join-Path $RepoDir "_config.db"

function Log($msg)  { Write-Host "[update] $msg" -ForegroundColor Green }
function Warn($msg) { Write-Host "[update] $msg" -ForegroundColor Yellow }

# ── 1. Check whether auto-update is enabled ──────────────────────────
$Enabled = python -c @"
import sqlite3
try:
    db = sqlite3.connect(r'$ConfigDb')
    row = db.execute(\"SELECT value FROM config WHERE key='auto_update'\").fetchone()
    print(row[0] if row else '0')
except Exception:
    print('0')
"@ 2>$null
if ($Enabled -ne "1") {
    Warn "Auto-update is disabled. Enable it in Advanced settings or re-run install.ps1."
    exit 0
}

Log "Messages update check — $(Get-Date)"

# ── 2. Fetch latest tags ──────────────────────────────────────────────
try {
    git -C $RepoDir fetch --tags --quiet 2>$null
} catch {
    Warn "Could not reach remote repository — skipping update."
    exit 0
}

# ── 3. Determine current and latest version ──────────────────────────
$Current = git -C $RepoDir describe --tags --exact-match 2>$null
if (-not $Current) { $Current = git -C $RepoDir describe --tags 2>$null }
if (-not $Current) { $Current = git -C $RepoDir rev-parse --short HEAD 2>$null }
if (-not $Current) { $Current = "unknown" }

$Latest = (git -C $RepoDir tag --sort=-version:refname 2>$null | Select-Object -First 1)
if (-not $Latest) {
    Warn "No release tags found — nothing to update."
    exit 0
}

$Behind = [int](git -C $RepoDir rev-list --count "HEAD..refs/tags/$Latest" 2>$null)
if ($Behind -eq 0) {
    Log "Already at latest release ($Current). Nothing to do."
    exit 0
}

Log "Update available: $Current → $Latest"
Log "Applying..."

# ── 4. Checkout the new release ───────────────────────────────────────
git -C $RepoDir checkout $Latest --quiet

# ── 5. Update Python dependencies ────────────────────────────────────
$PipExe = "$RepoDir\venv\Scripts\pip.exe"
if (Test-Path $PipExe) {
    & $PipExe install --quiet -r "$RepoDir\requirements.txt"
} else {
    Warn "venv not found — skipping pip install."
}

# ── 6. Restart the scheduled task ────────────────────────────────────
try {
    Stop-ScheduledTask  -TaskName $ServiceName -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    Start-ScheduledTask -TaskName $ServiceName
    Log "Service restarted."
} catch {
    Warn "Could not restart task — restart manually."
}

Log "Updated Messages to $Latest"
