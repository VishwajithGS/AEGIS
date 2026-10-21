# Shared helpers for the AEGIS PowerShell scripts (Windows PowerShell 5.1+).
$ErrorActionPreference = "Stop"
$Global:AegisRoot = Split-Path -Parent $PSScriptRoot

function Write-Step([string]$msg) { Write-Host "==> $msg" -ForegroundColor Cyan }
function Write-Ok([string]$msg) { Write-Host "    OK  $msg" -ForegroundColor Green }
function Write-Warn2([string]$msg) { Write-Host "    !!  $msg" -ForegroundColor Yellow }

function Assert-Docker {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "Docker CLI not found. Install Docker Desktop: https://docs.docker.com/desktop/setup/install/windows-install/"
    }
    $job = Start-Job { docker version --format "{{.Server.Version}}" 2>$null }
    if (-not (Wait-Job $job -Timeout 25)) {
        Stop-Job $job; Remove-Job $job -Force
        throw "Docker engine is not responding. Start Docker Desktop (and check free disk space on the drive that holds its disk image)."
    }
    $ver = Receive-Job $job; Remove-Job $job -Force
    if (-not $ver) { throw "Docker engine is not running. Start Docker Desktop and retry." }
    Write-Ok "Docker engine $ver"
}

# Simple (non-advanced) functions on purpose: every argument, including ones that
# look like flags (-d, -m, --no-deps), is passed through unchanged via $args.
function Invoke-Compose {
    Push-Location $Global:AegisRoot
    try {
        & docker compose @args
        if ($LASTEXITCODE -ne 0) { throw "docker compose $($args -join ' ') failed with exit code $LASTEXITCODE" }
    } finally { Pop-Location }
}

# Run a one-off command in the aegis-spark tools container (project mounted at /app).
# Usage: Invoke-Tool [-NoDeps] <command...>
function Invoke-Tool {
    $rest = @($args)
    $all = @("run", "--rm")
    if ($rest.Count -gt 0 -and $rest[0] -eq "-NoDeps") {
        $all += "--no-deps"
        $rest = if ($rest.Count -gt 1) { $rest[1..($rest.Count - 1)] } else { @() }
    }
    $all += "tools"
    $all += $rest
    Invoke-Compose @all
}

function Wait-Healthy([string]$service, [int]$timeoutSec = 240) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    Push-Location $Global:AegisRoot
    try {
        while ((Get-Date) -lt $deadline) {
            $id = (docker compose ps -q $service) 2>$null
            if ($id) {
                $state = (docker inspect --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $id) 2>$null
                $hasHealthcheck = $service -in @("kafka", "hbase")
                if (($state -eq "healthy") -or ((-not $hasHealthcheck) -and ($state -eq "running"))) { Write-Ok "$service is $state"; return }
                Write-Host "    ... $service is $state"
            }
            Start-Sleep -Seconds 5
        }
    } finally { Pop-Location }
    throw "$service did not become healthy within $timeoutSec s (see: docker compose logs $service)"
}
