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
