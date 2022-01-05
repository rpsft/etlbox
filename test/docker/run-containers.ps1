#!/usr/bin/env pwsh

& docker run --rm -d --cap-add SYS_PTRACE -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=YourStrong@Passw0rd" -e "MSSQL_PID=Developer" -p 1433:1433 --name localmssql -d mcr.microsoft.com/azure-sql-edge
if ( $LastExitCode -ne 0 ) { exit $LastExitCode }


& docker run --rm -d -e "MYSQL_ROOT_HOST=%" -e "MYSQL_ROOT_PASSWORD=etlboxpassword" -p 3306:3306 --name localmysql -d mysql/mysql-server
if ( $LastExitCode -ne 0 ) { exit $LastExitCode }

& docker run --rm -d -e "POSTGRES_PASSWORD=etlboxpassword" -p 5432:5432 --name localpostgres -d postgres
if ( $LastExitCode -ne 0 ) { exit $LastExitCode }
