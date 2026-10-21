<#
.SYNOPSIS  Start Spark Structured Streaming (Kafka -> Parquet/HBase/Kafka) in the background, or drain once.
.EXAMPLE   scripts\start_streaming.ps1            # continuous, detached; Spark UI http://localhost:4040
.EXAMPLE   scripts\start_streaming.ps1 -Follow    # continuous and show logs
.EXAMPLE   scripts\start_streaming.ps1 -Once      # process everything available, then stop (availableNow)
#>
param([switch]$Follow, [switch]$Once, [switch]$Reset)
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot
Assert-Docker
if ($Once) {
    $cmd = @("python", "-m", "spark_jobs.streaming_pipeline", "--available-now")
    if ($Reset) { $cmd += "--reset" }
    Write-Step "Running the streaming queries once (availableNow)"
    Invoke-Tool @cmd
    return
}
if ($Reset) {
    Write-Step "Removing streaming checkpoints (queries restart from streaming.starting_offsets)"
    Invoke-Compose stop streaming
    Remove-Item -Recurse -Force "$AegisRoot\data\checkpoints\bronze_dq", "$AegisRoot\data\checkpoints\silver", "$AegisRoot\data\checkpoints\zone_risk" -ErrorAction SilentlyContinue
}
Write-Step "Starting the streaming service"
Invoke-Compose up -d streaming
Write-Ok "streaming started (status: data\state\streaming\status.json, Spark UI: http://localhost:4040)"
if ($Follow) { Invoke-Compose logs -f streaming }
