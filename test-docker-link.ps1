#!/usr/bin/env pwsh

docker run -it --rm -v "${pwd}:/tmp/project" --link localmssql:localmssql --link localmysql:localmysql --link localpostgres:localpostgres mcr.microsoft.com/dotnet/sdk:6.0 bash