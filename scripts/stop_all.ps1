<#
.SYNOPSIS  Stop AEGIS safely. Streaming gets up to 90 s to finish its current micro-batch (SIGTERM).
.EXAMPLE   scripts\stop_all.ps1          # stop containers (data, topics and tables are kept)
.EXAMPLE   scripts\stop_all.ps1 -Down    # also remove containers (volumes kept)
.EXAMPLE   scripts\stop_all.ps1 -Down -DeleteVolumes   # ALSO DELETES Kafka topics/messages and HBase tables
#>
param([switch]$Down, [switch]$DeleteVolumes)
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot
Assert-Docker
Write-Step "Stopping producers, API and streaming (graceful)"
Invoke-Compose stop producers api streaming
Write-Step "Stopping Kafka and HBase"
Invoke-Compose stop kafka hbase
if ($Down) {
    if ($DeleteVolumes) {
        Write-Warn2 "Removing containers AND volumes (Kafka data, HBase tables)"
        Invoke-Compose down -v
    } else {
        Invoke-Compose down
    }
}
Write-Ok "stopped (local files in data\ are untouched)"
