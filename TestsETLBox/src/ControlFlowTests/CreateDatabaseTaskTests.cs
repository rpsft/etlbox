using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class CreateDatabaseTaskTests
    {
        public SqlConnectionManager MasterConnection => new SqlConnectionManager(Config.SqlConnectionString("ControlFlow").GetMasterConnection());
        public static IEnumerable<object[]> SqlConnectionsWithMaster() => new[] {
                    new object[] { (IConnectionManager)new SqlConnectionManager(Config.SqlConnection.ConnectionString("ControlFlow").GetMasterConnection()) },
                    new object[] { (IConnectionManager)new MySqlConnectionManager(Config.MySqlConnection.ConnectionString("ControlFlow").GetMasterConnection()) },
        };
        public CreateDatabaseTaskTests()
        { }

        [Theory, MemberData(nameof(SqlConnectionsWithMaster))]
        public void CreateSimple(IConnectionManager connection)
        {
            //Arrange
            string dbName = "ETLBox_"+HashHelper.RandomString(10);
            var dbListBefore = GetDatabaseListTask.List(connection);
            Assert.DoesNotContain<string>(dbName, dbListBefore);

            //Act
            CreateDatabaseTask.Create(connection, dbName);

            //Assert
            var dbListAfter = GetDatabaseListTask.List(connection);
            Assert.Contains<string>(dbName, dbListAfter);

            //Cleanup
            DropDatabaseTask.Drop(connection, dbName);
        }

        public SQLiteConnectionManager SQLiteConnection => Config.SQLiteConnection.ConnectionManager("ControlFlow");

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => CreateDatabaseTask.Create(SQLiteConnection, "Test")
                );
        }
    }
}
