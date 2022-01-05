using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests
{
    public class ConnectionStringTests
    {
        
        [Fact]
        public void NormalSqlServerString()
        {
            //Arrange
            var connString = new SqlConnectionString();

            //Act
            connString.Value = "Server=.;User Id=test;Password=test;Database=TestDB;";
            var withoutDbName = connString.CloneWithoutDbName().Value;
            var withMaster = connString.CloneWithMasterDbName().Value;
            var newDbName = connString.CloneWithNewDbName("test").Value;

            //Assert
            Assert.Equal("Data Source=.;User ID=test;Password=test", withoutDbName);
            Assert.Equal("Data Source=.;Initial Catalog=master;User ID=test;Password=test", withMaster);
            Assert.Equal("Data Source=.;Initial Catalog=test;User ID=test;Password=test", newDbName);

        }

        [Fact]
        public void NormalSQLiteString()
        {
            //Arrange
            var connString = new SQLiteConnectionString
            {
                Value = $"Data Source=.{Path.PathSeparator}db{Path.PathSeparator}SQLiteControlFlow.db;"
            };

            //Act
            var withoutDbName = connString.CloneWithoutDbName().Value;
            var newDbName = connString.CloneWithNewDbName("test").Value;

            //Assert
            Assert.Equal("", withoutDbName);
            Assert.Equal("Data Source=test", newDbName);
            Assert.Throws<ETLBoxNotSupportedException>(() =>
                connString.CloneWithMasterDbName().Value
            );
        }

        [Fact]
        public void NormalPostgresString()
        {
            //Arrange
            var connString = new PostgresConnectionString();

            //Act
            connString.Value = "Server=10.211.55.2;Database=TestDb;User Id=postgres;Password=etlboxpassword;";
            var withoutDbName = connString.CloneWithoutDbName().Value;
            var withMaster = connString.CloneWithMasterDbName().Value;
            var newDbName = connString.CloneWithNewDbName("test").Value;


            //Assert
            Assert.Equal("Host=10.211.55.2;Username=postgres;Password=etlboxpassword", withoutDbName);
            Assert.Equal("Host=10.211.55.2;Database=postgres;Username=postgres;Password=etlboxpassword", withMaster);
            Assert.Equal("Host=10.211.55.2;Database=test;Username=postgres;Password=etlboxpassword", newDbName);

        }

        [Fact]
        public void NormalMySqlString()
        {
            //Arrange
            var connString = new MySqlConnectionString();

            //Act
            connString.Value = "Server=10.211.55.2;Database=TestDb;Uid=root;Pwd=etlboxpassword;";
            var withoutDbName = connString.CloneWithoutDbName().Value;
            var withMaster = connString.CloneWithMasterDbName().Value;
            var newDbName = connString.CloneWithNewDbName("test").Value;


            //Assert
            Assert.Equal("server=10.211.55.2;user id=root;password=etlboxpassword", withoutDbName);
            Assert.Equal("server=10.211.55.2;database=mysql;user id=root;password=etlboxpassword", withMaster);
            Assert.Equal("server=10.211.55.2;database=test;user id=root;password=etlboxpassword", newDbName);

        }

    }
}
