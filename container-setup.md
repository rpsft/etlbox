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

docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=YourStrong@Passw0rd" -e "MSSQL_PID='Developer'"" -p 1433:1433 --name localmssql -d mcr.microsoft.com/mssql/server
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

### ODBC on Mac
```
brew install unixodbc
brew install mdbtools
```