<#
.SYNOPSIS  Scripted end-to-end verification (services -> Kafka -> Streaming -> ETL -> Parquet/HBase -> ML -> API).
           Writes data\reports\e2e_report.md and .json with PASS / FAIL / BLOCKED per step.
.NOTES     The check runs its own streaming pass on the shared checkpoints and its own producer,
           so a running `streaming` / `producers` service is paused (graceful stop) for the
           duration and started again afterwards.
#>
param([int]$LiveSeconds = 60, [double]$Rate = 10)
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot
Assert-Docker
& "$PSScriptRoot\start_infrastructure.ps1"

Push-Location $AegisRoot
$running = @(docker compose ps --status running --services 2>$null)
Pop-Location
$paused = @(@("streaming", "producers") | Where-Object { $running -contains $_ })
if ($paused.Count -gt 0) {
    Write-Step "Pausing running services that would conflict with the check: $($paused -join ', ')"
    $stopArgs = @("stop") + $paused
    Invoke-Compose @stopArgs
}

$code = 1
try {
    Write-Step "Starting the API container"
    Invoke-Compose up -d api
    Write-Step "Running the end-to-end check (this takes several minutes)"
    Push-Location $AegisRoot
    & docker compose run --rm tools python scripts/e2e_check.py --live-seconds $LiveSeconds --rate $Rate --api-url http://api:8000
    $code = $LASTEXITCODE
    Pop-Location
    Get-Content "$AegisRoot\data\reports\e2e_report.md"
} finally {
    if ($paused.Count -gt 0) {
        Write-Step "Restarting paused services: $($paused -join ', ')"
        $startArgs = @("start") + $paused
        Invoke-Compose @startArgs
    }
}
exit $code
