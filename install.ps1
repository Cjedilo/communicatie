# Messages — installer for Windows (PowerShell)
# Called by install.cmd or directly: powershell -ExecutionPolicy Bypass -File install.ps1

$ErrorActionPreference = "Stop"
$RepoDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ServiceName = "Messages"
$ConfigDb = Join-Path $RepoDir "_config.db"

function Step($msg)  { Write-Host "`n==> $msg" -ForegroundColor Green }
function Ok($msg)    { Write-Host "  OK $msg" -ForegroundColor Green }
function Warn($msg)  { Write-Host "  WARNING: $msg" -ForegroundColor Yellow }
function Fail($msg)  { Write-Host "  ERROR: $msg" -ForegroundColor Red; exit 1 }

Write-Host ""
Write-Host "Messages - self-hosted federated chat" -ForegroundColor White
Write-Host "--------------------------------------"

# ── 1. Python check ───────────────────────────────────────────────
Step "Checking Python version"
$pyver = python --version 2>&1
if ($pyver -notmatch "Python 3\.(1[2-9]|[2-9]\d)") { Fail "Python 3.12+ required (found: $pyver)" }
Ok $pyver

# ── 2. Venv + deps ───────────────────────────────────────────────
Step "Installing dependencies"
python -m venv "$RepoDir\venv"
& "$RepoDir\venv\Scripts\pip.exe" install --quiet --upgrade pip
& "$RepoDir\venv\Scripts\pip.exe" install --quiet -r "$RepoDir\requirements.txt"
Ok "Dependencies installed"

# ── Helper: check port ────────────────────────────────────────────
function Test-PortFree($port) {
    $connections = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    return ($null -eq $connections)
}

# ── 3. Ports ─────────────────────────────────────────────────────
Step "Configuring ports"

$Port = 443
if (-not (Test-PortFree 443)) {
    Warn "Port 443 is already in use."
    $Port = Read-Host "  Enter HTTPS port [8443]"
    if (-not $Port) { $Port = 8443 }
    $Port = [int]$Port
}
Ok "HTTPS port: $Port"

$PortHttp = 80
if (-not (Test-PortFree 80)) {
    Warn "Port 80 is already in use."
    $PortHttp = Read-Host "  Enter HTTP redirect port (0 to disable) [0]"
    if (-not $PortHttp) { $PortHttp = 0 }
    $PortHttp = [int]$PortHttp
}
Ok "HTTP redirect port: $PortHttp"

# ── 4. Upload directory ───────────────────────────────────────────
Step "Configuring storage"
$DefaultUpload = "C:\ProgramData\Messages\uploads"
$UploadDir = Read-Host "  Upload directory [$DefaultUpload]"
if (-not $UploadDir) { $UploadDir = $DefaultUpload }

try {
    New-Item -ItemType Directory -Force -Path $UploadDir | Out-Null
    Ok "Upload directory: $UploadDir"
} catch {
    Fail "Cannot create '$UploadDir': $_"
}

# ── 5. Setup secret + config ──────────────────────────────────────
Step "Generating setup secret"
$SetupSecret = python -c "import secrets; print(secrets.token_urlsafe(32))"

python - @"
import sqlite3
db = sqlite3.connect(r'$ConfigDb')
db.execute('CREATE TABLE IF NOT EXISTS config(key TEXT PRIMARY KEY, value TEXT)')
for k, v in [
    ('port',         '$Port'),
    ('port_http',    '$PortHttp'),
    ('upload_dir',   r'$UploadDir'),
    ('setup_secret', '$SetupSecret'),
]:
    db.execute('INSERT OR REPLACE INTO config(key,value) VALUES(?,?)', (k, v))
db.commit()
db.close()
"@
Ok "Config stored"

# ── 6. Detect local IP ────────────────────────────────────────────
$DetectedIp = python -c @"
import socket
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    print(s.getsockname()[0])
    s.close()
except:
    print('localhost')
"@

# ── 7. Windows Task Scheduler (auto-start) ────────────────────────
Step "Registering auto-start task"
$PythonExe = "$RepoDir\venv\Scripts\python.exe"
$TaskAction = New-ScheduledTaskAction -Execute $PythonExe -Argument "main.py" -WorkingDirectory $RepoDir
$TaskTrigger = New-ScheduledTaskTrigger -AtStartup
$TaskSettings = New-ScheduledTaskSettingsSet -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
try {
    Register-ScheduledTask -TaskName $ServiceName -Action $TaskAction `
        -Trigger $TaskTrigger -Settings $TaskSettings -RunLevel Highest `
        -Force | Out-Null
    Start-ScheduledTask -TaskName $ServiceName
    Ok "Task '$ServiceName' registered and started"
} catch {
    Warn "Could not register scheduled task (run as Administrator for auto-start)."
    Warn "Start manually: $PythonExe $RepoDir\main.py"
}

# ── 8. Auto-update (optional) ────────────────────────────────────────
Step "Automatic updates"
Write-Host "   Messages can automatically install new tagged releases every night."
Write-Host "   Only official releases (git tags) are installed — never untagged commits."
$AutoChoice = Read-Host "  Enable automatic updates? [y/N]"

if ($AutoChoice -match "^[Yy]$") {
    python - @"
import sqlite3
db = sqlite3.connect(r'$ConfigDb')
db.execute("INSERT OR REPLACE INTO config(key,value) VALUES('auto_update','1')")
db.commit()
db.close()
"@
    $UpdateScript  = "$RepoDir\update.ps1"
    $UpdateAction  = New-ScheduledTaskAction -Execute "powershell.exe" `
                         -Argument "-ExecutionPolicy Bypass -NonInteractive -File `"$UpdateScript`""
    $UpdateTrigger = New-ScheduledTaskTrigger -Daily -At "03:00AM"
    $UpdateSettings = New-ScheduledTaskSettingsSet -StartWhenAvailable
    try {
        Register-ScheduledTask -TaskName "Messages-Update" -Action $UpdateAction `
            -Trigger $UpdateTrigger -Settings $UpdateSettings -RunLevel Highest `
            -Force | Out-Null
        Ok "Auto-update task registered (runs nightly at 03:00)"
    } catch {
        Warn "Could not register auto-update task (run as Administrator)."
    }
} else {
    python - @"
import sqlite3
db = sqlite3.connect(r'$ConfigDb')
db.execute("INSERT OR REPLACE INTO config(key,value) VALUES('auto_update','0')")
db.commit()
db.close()
"@
    Ok "Auto-update disabled (can be enabled later in Advanced settings)"
}

# ── 9. Done ───────────────────────────────────────────────────────
if ($Port -eq 443) { $SetupUrl = "https://${DetectedIp}/setup/$SetupSecret" }
else               { $SetupUrl = "https://${DetectedIp}:${Port}/setup/$SetupSecret" }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host " Messages installed successfully!" -ForegroundColor White
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host " Open this URL to claim your server as owner:" -ForegroundColor White
Write-Host ""
Write-Host "   $SetupUrl" -ForegroundColor Cyan
Write-Host ""
Write-Host " Keep this URL safe - it works once." -ForegroundColor Yellow
Write-Host " The URL is also logged each time the server starts"
Write-Host " until the server has been claimed."
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
