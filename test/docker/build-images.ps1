#!/usr/bin/env pwsh

$docker='nerdctl'
& $docker login

#$env:DOCKER_BUILDKIT=1
& $docker image build mssql --progress=plain -t "etlbox-mssql"
