#!/usr/bin/env pwsh

nerdctl run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=YourStrong@Passw0rd" -e "MSSQL_PID=Developer" -p 1433:1433 --name localmssql -d etlbox-mssql
