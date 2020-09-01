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

## MariaDb
```bash
docker run --name localmaria -p 3307:3306 -e MYSSQL_ROOT_HOST='%' -e MYSQL_ROOT_PASSWORD='etlboxpassword' -d mariadb:latest
```

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


## Oracle

Read the manual here: https://github.com/oracle/docker-images/tree/master/OracleDatabase/SingleInstance
Go to https://container-registry.oracle.com
Sign in 
Go to the database you want to download
Accept license
copy docker pull command

Until license is not accepted in web interface, docker pull won't authorize download

```
docker login container-registry.oracle.com/database/standard:latest
docker pull container-registry.oracle.com/database/standard:latest
docker run --name localoracle -d -p 1521:1521 container-registry.oracle.com/database/standard:latest
```

For `docker login`, use same user and password as for website
Login in to localhost
SID: ORCLCDB
User: sys   (as sysdba)
Password: Oradoc_db1

After logged in as DBA, create a user:
```
alter session set "_ORACLE_SCRIPT"=true;  
create user etlbox identified by etlboxpassword;
grant dba to etlbox;
```

## Neo4J

```
docker run --name localneo4j -p 7474:7474 -p 7687:7687 -d -e NEO4J_AUTH=none neo4j
```

Then run `docker start localneo4j` and wait for the container to start.
Go to http://localhost:7474/ in the browser - choose "No authentication" and connect to the neo4j database. 
Connection via bolt driver: `bolt://localhost:7687`

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

Oracle
- installing this driver is really is a pain in the a**...
- go to https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html
- download basic package 
- download odbc package
- unzip both, merge into one folder
- copy somewhere on windows machine, etc. C:/Oracle
- add an entry to the PATH system environment variable pointing to this folder
- go into this folder, run `odbc_install.exe` as administrator
- the odbc should now pop up in Odbc64 manager


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