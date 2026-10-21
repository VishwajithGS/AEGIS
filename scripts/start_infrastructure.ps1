<#
.SYNOPSIS  Start Kafka and HBase, wait until healthy, create Kafka topics and HBase tables.
#>
. "$PSScriptRoot\common.ps1"
Set-Location $AegisRoot
Assert-Docker
Write-Step "Starting Kafka (apache/kafka:4.3.1, KRaft) and HBase (2.5.16 standalone + Thrift)"
Invoke-Compose up -d kafka hbase
Wait-Healthy kafka 180
Wait-Healthy hbase 300
Write-Step "Creating Kafka topics"
Invoke-Tool -NoDeps python -m producers.kafka_admin create
Write-Step "Creating HBase tables"
Invoke-Tool -NoDeps python hbase/create_tables.py
Write-Host "`nInfrastructure ready. Kafka: localhost:9092 | HBase UI: http://localhost:16010 | Thrift: localhost:9090" -ForegroundColor Green
