#!/usr/bin/env pwsh

$docker='docker'
& $docker login

#$env:DOCKER_BUILDKIT=1
& $docker image build mssql --progress=plain -t "etlbox-mssql"

& docker build -t my-clickhouse-image .

