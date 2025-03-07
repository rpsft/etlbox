#!/usr/bin/env pwsh
param (
  [Alias("c")]
  [switch]
  $clear = $false
)

$kafkaPath = [System.IO.Path]::Combine($PSScriptRoot, "docker", "kafka.yml")

if ($clear)
{
  # Stop running containers
  $containerNames = @("localmssql", "localmysql", "localpostgres", "localclickhouse")
  $runningContainers = docker ps -q -f "name=$( $containerNames -join '|' )"

  if ($runningContainers)
  {
    docker stop $runningContainers
    if ($LASTEXITCODE -ne 0)
    {
      throw "Failed to stop containers with exit code: $LASTEXITCODE"
    }
  }

  # Remove existing containers with the same names
  $existingContainers = docker ps -a -q -f "name=$( $containerNames -join '|' )"
  if ($existingContainers)
  {
    docker rm $existingContainers
    if ($LASTEXITCODE -ne 0)
    {
      throw "Failed to remove containers with exit code: $LASTEXITCODE"
    }
  }

  # Bring down Kafka services
  & docker-compose -f $kafkaPath down -v
  if ($LASTEXITCODE -ne 0)
  {
    throw "Failed to bring down Kafka with exit code: $LASTEXITCODE"
  }
}

& docker run -d --cap-add SYS_PTRACE -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=YourStrong@Passw0rd" -e "MSSQL_PID=Developer" -p 1433:1433 --name localmssql -d mcr.microsoft.com/azure-sql-edge
if ($LASTEXITCODE -ne 0)
{
  throw "Failed to start SQL Server Edge container. Exit code: $LASTEXITCODE"
}

& docker run -d -e "MYSQL_ROOT_HOST=%" -e "MYSQL_ROOT_PASSWORD=etlboxpassword" -p 3306:3306 --name localmysql -d mysql/mysql-server
if ($LASTEXITCODE -ne 0)
{
  throw "Failed to start MySQL container. Exit code: $LASTEXITCODE"
}

& docker run -d -e "POSTGRES_PASSWORD=etlboxpassword" -e "LANG=en_US.utf8" -p 5432:5432 --name localpostgres -d postgres
if ($LASTEXITCODE -ne 0)
{
  throw "Failed to start PostgreSQL container. Exit code: $LASTEXITCODE"
}

& docker run -d --name localclickhouse -p 8123:8123 -p 9000:9000 -e "CLICKHOUSE_USER=clickhouse" -e "CLICKHOUSE_PASSWORD=Qwe123456" -e "CLICKHOUSE_MAX_CONNECTIONS=100" clickhouse/clickhouse-server
if ($LASTEXITCODE -ne 0)
{
  throw "Failed to start ClickHouse container. Exit code: $LASTEXITCODE"
}

& docker-compose -f $kafkaPath up -d
if ($LASTEXITCODE -ne 0)
{
  throw "Failed to start Kafka containers using docker-compose. Exit code: $LASTEXITCODE"
}
