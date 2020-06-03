using ETLBox;
using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.Helper;
using ETLBox.MySql;
using ETLBox.Postgres;
using ETLBox.SqlServer;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class DropDatabaseTaskTests
    {
        public SqlConnectionManager MasterConnection => new SqlConnectionManager(Config.SqlConnection.ConnectionString("ControlFlow").CloneWithMasterDbName());
        public static IEnumerable<object[]> SqlConnectionsWithMaster() => new[] {
                    new object[] { (IConnectionManager)new SqlConnectionManager(Config.SqlConnection.ConnectionString("ControlFlow").CloneWithMasterDbName()) },
                    new object[] { (IConnectionManager)new PostgresConnectionManager(Config.PostgresConnection.ConnectionString("ControlFlow").CloneWithMasterDbName()) },
                    new object[] { (IConnectionManager)new MySqlConnectionManager(Config.MySqlConnection.ConnectionString("ControlFlow").CloneWithMasterDbName()) },
        };
        public DropDatabaseTaskTests()
        { }

        [Theory, MemberData(nameof(SqlConnectionsWithMaster))]
        public void Drop(IConnectionManager connection)
        {
            //Arrange
            string dbName = "ETLBox_" + TestHashHelper.RandomString(10);
            CreateDatabaseTask.Create(connection, dbName);
            bool existsBefore = IfDatabaseExistsTask.IsExisting(connection, dbName);

            //Act
            DropDatabaseTask.Drop(connection, dbName);

            //Assert
            bool existsAfter = IfDatabaseExistsTask.IsExisting(connection, dbName);
            Assert.True(existsBefore);
            Assert.False(existsAfter);
        }

        [Theory, MemberData(nameof(SqlConnectionsWithMaster))]
        public void DropIfExists(IConnectionManager connection)
        {
            //Arrange
            string dbName = "ETLBox_" + TestHashHelper.RandomString(10);
            DropDatabaseTask.DropIfExists(connection, dbName);
            CreateDatabaseTask.Create(connection, dbName);
            bool existsBefore = IfDatabaseExistsTask.IsExisting(connection, dbName);

            //Act
            DropDatabaseTask.DropIfExists(connection, dbName);

            //Assert
            bool existsAfter = IfDatabaseExistsTask.IsExisting(connection, dbName);
            Assert.True(existsBefore);
            Assert.False(existsAfter);
        }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => DropDatabaseTask.Drop(Config.SQLiteConnection.ConnectionManager("ControlFlow"), "Test")
                );
        }

    }
}
