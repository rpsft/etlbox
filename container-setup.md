# Docker setup

## MySql

```bash
docker run --name localmysql -p 3306:3306 -e MYSQL_ROOT_HOST='%' -e MYSQL_ROOT_PASSWORD='etlboxpassword' -d mysql/mysql-server

docker exec -it localmysql bash -l

mysql -uroot -petlboxpassword

ALTER USER 'root' IDENTIFIED WITH mysql_native_password BY 'etlboxpassword';
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'etlboxpassword';
FLUSH PRIVILEGES;
```

Login to 
localhost (3306)
User: root
Password: etlboxpassword


## Postgres

```
docker run --name localpostgres -e POSTGRES_PASSWORD=etlboxpassword -d -p 5432:5432 postgres
```

Login to
localhost (5432)
User: postgres
Password: etlboxpassword
Database: postgres


## SqlServer

```
docker login

docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=YourStrong@Passw0rd" -p 1433:1433 --name localmssql -d mcr.microsoft.com/mssql/server
```

Login to
localhost (1433)
User: sa
Password: YourStrong@Passw0rd


## Odbc setup

For excel: download latest driver
- Odbc driver needs to be 64bit if using 64bit .NET core 
- (Visual Studio 2019 16.4 changed default behvaiour for xunit Tests - they now run with .NET Core 32bit versions)
- Recommended: Driver Access >2016 https://www.microsoft.com/en-us/download/details.aspx?id=54920
- Old driver: Driver Access >2010 https://www.microsoft.com/en-us/download/details.aspx?id=13255
- Start "ODBC data sources 64-bit" , just add "Microsoft Access driver" as System DNS
 - enter a data source name,e.g. Accesss - no further configuration needed
- Everything else is derived from Connection String

SqlServer
- just add the Odbc Driver 17 for Sql Server
- the odbc driver is installed with Sql Server database
- no further configuration need

Postgres
- download latests ODBC driver for Postgres https://www.postgresql.org/ftp/odbc/versions/
- just add Postgres Sql UNICODE (x64) 
- no further configuration needed
- the connection string referes to UNICODE (Driver={PostgreSQL UNICODE}), if you use ANSI adapt the connection string (Driver={PostgreSQL ANSI})
- Driver={PostgreSQL} is not supported any more?


## OleDb drivers
- OleDb driver for Sql Server was installed with database
- MySql has no official OleDb Driver
- OleDb driver for Postgres: http://www.pgoledb.com

## Line break settings

- current tests and files for comparison are created under windows and have \r\n as LineBreak (instead of \n only)
- if cloned under mac os or linux with git clone, the line breaks will be converted automatically into \n
- now tests will fail, because the as-is files created in windows will have different line breaks that than the to-be files in the cloned project
- to avoid the converting the line breaks, you need to set the core.autocrlf=true in global git config:
```
git config --global --add core.autocrlf true
```
After doing this, you need to clone the project (if you already cloned it, remove it and clone it again)