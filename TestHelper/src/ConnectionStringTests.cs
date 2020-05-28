using ETLBox;
using ETLBox.MySql;
using ETLBox.Postgres;
using ETLBox.SQLite;
using ETLBox.SqlServer;
using Xunit;

namespace ETLBoxTests
{
    public class ConnectionStringTests
    {

        [Fact]
        public void NormalSqlServerString()
        {
            //Arrange
            SqlConnectionString connString = new SqlConnectionString();

            //Act
            connString.Value = "Server=.;User Id=test;Password=test;Database=TestDB;";
            string withoutDbName = connString.CloneWithoutDbName().Value;
            string withMaster = connString.CloneWithMasterDbName().Value;
            string newDbName = connString.CloneWithNewDbName("test").Value;

            //Assert
            Assert.Equal("Data Source=.;User ID=test;Password=test", withoutDbName);
            Assert.Equal("Data Source=.;Initial Catalog=master;User ID=test;Password=test", withMaster);
            Assert.Equal("Data Source=.;Initial Catalog=test;User ID=test;Password=test", newDbName);

        }

        [Fact]
        public void NormalSQLiteString()
        {
            //Arrange
            SQLiteConnectionString connString = new SQLiteConnectionString();

            //Act
            connString.Value = "Data Source=.\\db\\SQLiteControlFlow.db;Version=3;";
            string withoutDbName = connString.CloneWithoutDbName().Value;
            string newDbName = connString.CloneWithNewDbName("test").Value;

            //Assert
            Assert.Equal("version=3", withoutDbName);
            Assert.Equal("data source=test;version=3", newDbName);
            Assert.Throws<ETLBoxNotSupportedException>(() =>
                connString.CloneWithMasterDbName().Value
            );


        }

        [Fact]
        public void NormalPostgresString()
        {
            //Arrange
            PostgresConnectionString connString = new PostgresConnectionString();

            //Act
            connString.Value = "Server=10.211.55.2;Database=TestDb;User Id=postgres;Password=etlboxpassword;";
            string withoutDbName = connString.CloneWithoutDbName().Value;
            string withMaster = connString.CloneWithMasterDbName().Value;
            string newDbName = connString.CloneWithNewDbName("test").Value;


            //Assert
            Assert.Equal("Host=10.211.55.2;Username=postgres;Password=etlboxpassword", withoutDbName);
            Assert.Equal("Host=10.211.55.2;Database=postgres;Username=postgres;Password=etlboxpassword", withMaster);
            Assert.Equal("Host=10.211.55.2;Database=test;Username=postgres;Password=etlboxpassword", newDbName);

        }

        [Fact]
        public void NormalMySqlString()
        {
            //Arrange
            MySqlConnectionString connString = new MySqlConnectionString();

            //Act
            connString.Value = "Server=10.211.55.2;Database=TestDb;Uid=root;Pwd=etlboxpassword;";
            string withoutDbName = connString.CloneWithoutDbName().Value;
            string withMaster = connString.CloneWithMasterDbName().Value;
            string newDbName = connString.CloneWithNewDbName("test").Value;


            //Assert
            Assert.Equal("server=10.211.55.2;user id=root;password=etlboxpassword", withoutDbName);
            Assert.Equal("server=10.211.55.2;database=mysql;user id=root;password=etlboxpassword", withMaster);
            Assert.Equal("server=10.211.55.2;database=test;user id=root;password=etlboxpassword", newDbName);

        }

    }
}
