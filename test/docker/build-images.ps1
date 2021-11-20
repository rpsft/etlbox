#!/usr/bin/env pwsh

docker login

$env:DOCKER_BUILDKIT=1
docker build mssql --progress=plain -t "etlbox-mssql"