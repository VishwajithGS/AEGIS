<#
.SYNOPSIS  Build and serve the HYBRID data root (data_hybrid\): real rainfall and official events from data_real\,
           plus SYNTHETIC water level, traffic, citizen reports and incidents simulated from the real ERA5 rainfall.
           Runs the existing batch pipeline on it (ETL, real-data layers, MLlib training + zone predictions) and starts
           the hybrid API on http://localhost:8002 (dashboard: scripts\start_frontend.ps1 -Hybrid).
           data\ (synthetic) and data_real\ (real only) are not modified.
.EXAMPLE   scripts\run_hybrid_data.ps1             # generate + ETL + layers + model + api-hybrid
.EXAMPLE   scripts\run_hybrid_data.ps1 -NoApi
#>
param([switch]$NoApi, [int]$Seed = 42, [double]$WetnessTauH = 24)
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot
Assert-Docker
if (-not (Test-Path "$AegisRoot\data_real\raw\historical\manifest.json")) {
    throw "data_real\raw\historical\manifest.json not found. Run scripts\ingest_real_data.ps1 first."
}
$env_args = @("-e", "AEGIS__STORAGE__DATA_ROOT=data_hybrid", "-e", "AEGIS__HBASE__ENABLED=false")
function Invoke-HybridTool([string[]]$cmd) {
    $all = @("run", "--rm", "--no-deps") + $env_args + @("tools") + $cmd
    Invoke-Compose @all
}

# The batch jobs need ~3 GB; pause the synthetic live services (if running) like run_e2e.ps1 does.
Push-Location $AegisRoot
$running = @(docker compose ps --status running --services 2>$null)
Pop-Location
$paused = @(@("streaming", "producers") | Where-Object { $running -contains $_ })
if ($paused.Count -gt 0) {
    Write-Step "Pausing $($paused -join ', ') during the batch jobs (memory); restarted afterwards"
    $stopArgs = @("stop") + $paused
    Invoke-Compose @stopArgs
}
try {
    Write-Step "Synthetic water level, traffic, reports and incidents driven by the real ERA5 rainfall -> data_hybrid\"
    Invoke-HybridTool @("python", "-m", "producers.hybrid_generator", "--real-root", "data_real", "--out-root", "data_hybrid",
                        "--seed", "$Seed", "--wetness-tau-h", "$WetnessTauH")
    Write-Step "Batch ETL on hybrid data (bronze -> silver -> gold, data quality, rule-based risk)"
    Invoke-HybridTool @("python", "-m", "spark_jobs.etl_pipeline", "--input", "data_hybrid/raw/historical", "--no-stream-bronze")
    Write-Step "Real-data layers: observed daily rainfall (IMD, NOAA gauge) vs ERA5, official events"
    Invoke-HybridTool @("python", "-m", "spark_jobs.real_data_layers", "--input", "data_hybrid/raw/historical")
    Write-Step "MLlib model on the hybrid training set (SYNTHETIC labels) + per-zone predictions"
    Invoke-HybridTool @("python", "-m", "spark_jobs.model_training")
    Invoke-HybridTool @("python", "-m", "spark_jobs.model_inference", "--zones", "--source", "batch", "--no-hbase")
} finally {
    if ($paused.Count -gt 0) {
        Write-Step "Restarting $($paused -join ', ')"
        $startArgs = @("start") + $paused
        Invoke-Compose @startArgs
    }
}
if (-not $NoApi) {
    Write-Step "Starting the hybrid API (port 8002)"
    Invoke-Compose --profile hybrid up -d --force-recreate api-hybrid
    Write-Ok "API: http://localhost:8002/docs  |  dashboard: scripts\start_frontend.ps1 -Hybrid -> http://127.0.0.1:5175"
}
Write-Ok "reports: data_hybrid\reports\dq_report_batch.json, ml_evaluation.json, real_data_validation.json"
