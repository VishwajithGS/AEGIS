<#
.SYNOPSIS  Run the existing Spark batch pipeline on the REAL data root (data_real\) and build the real-data layers,
           then start the real-data API on http://localhost:8001 (dashboard: scripts\start_frontend.ps1 -RealData).
           Synthetic data (data\), HBase tables and Kafka topics are not touched.
.EXAMPLE   scripts\run_real_data.ps1            # ETL + real-data layers + api-real
.EXAMPLE   scripts\run_real_data.ps1 -NoApi
#>
param([switch]$NoApi)
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot
Assert-Docker
if (-not (Test-Path "$AegisRoot\data_real\raw\historical\manifest.json")) {
    throw "data_real\raw\historical\manifest.json not found. Run scripts\ingest_real_data.ps1 first."
}
# Real-data mode: own data root; no report source -> incidents indicator 'not observed'; no HBase writes.
$env_args = @("-e", "AEGIS__STORAGE__DATA_ROOT=data_real", "-e", "AEGIS__RISK__REPORTS_OBSERVED=false",
              "-e", "AEGIS__HBASE__ENABLED=false")
Write-Step "Batch ETL on real data (bronze -> silver -> gold, data quality)"
$etl = @("run", "--rm", "--no-deps") + $env_args + @("tools", "python", "-m", "spark_jobs.etl_pipeline",
         "--input", "data_real/raw/historical", "--no-stream-bronze")
Invoke-Compose @etl
Write-Step "Real-data layers: observed daily rainfall (IMD, NOAA gauge) vs ERA5, official events, validation report"
$layers = @("run", "--rm", "--no-deps") + $env_args + @("tools", "python", "-m", "spark_jobs.real_data_layers",
            "--input", "data_real/raw/historical")
Invoke-Compose @layers
if (-not $NoApi) {
    Write-Step "Starting the real-data API (port 8001)"
    Invoke-Compose --profile real up -d api-real
    Write-Ok "API: http://localhost:8001/docs  |  dashboard: scripts\start_frontend.ps1 -RealData -> http://127.0.0.1:5174"
}
Write-Ok "reports: data_real\reports\dq_report_batch.json, data_real\reports\real_data_validation.json"
