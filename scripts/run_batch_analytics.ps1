<#
.SYNOPSIS  Batch layer: ETL (bronze -> silver -> gold, DQ report, HBase load), RDD demo, Spark SQL, spatial and risk analytics.
.EXAMPLE   scripts\run_batch_analytics.ps1            # everything
.EXAMPLE   scripts\run_batch_analytics.ps1 -Only rdd  # one demo: etl | rdd | sql | spatial | risk
#>
param([ValidateSet("all", "etl", "rdd", "sql", "spatial", "risk")][string]$Only = "all", [switch]$NoHBase)
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot
Assert-Docker
if (-not (Test-Path "$AegisRoot\data\generated\historical\manifest.json")) {
    Write-Step "No generated data yet - generating"
    & "$PSScriptRoot\generate_data.ps1"
}
if ($Only -in "all", "etl") {
    Write-Step "Batch ETL (Spark): validation, mapping, de-duplication, gold tables, data-quality report"
    if ($NoHBase) { Invoke-Tool -NoDeps python -m spark_jobs.etl_pipeline } else { Invoke-Tool python -m spark_jobs.etl_pipeline --write-hbase }
}
if ($Only -in "all", "rdd") { Write-Step "Spark RDD transformations and actions"; Invoke-Tool -NoDeps python -m spark_jobs.rdd_demo }
if ($Only -in "all", "sql") { Write-Step "Spark SQL analytics"; Invoke-Tool -NoDeps python -m spark_jobs.sql_analytics }
if ($Only -in "all", "spatial") { Write-Step "Spatial analytics (Haversine, nearest stations, density)"; Invoke-Tool -NoDeps python -m spark_jobs.spatial_analytics --lat 13.05 --lon 80.22 --radius 3 }
if ($Only -in "all", "risk") { Write-Step "Rule-based zone risk ranking"; Invoke-Tool -NoDeps python -m spark_jobs.risk_analytics }
Write-Host "`nReports: data\reports\ (dq_report_batch.json, rdd_demo.json, sql\*.json, spatial_report.json)" -ForegroundColor Green
