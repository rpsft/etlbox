using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    public class IfDatabaseExistsTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        public IfDatabaseExistsTaskTests()
        { }

        [Theory, MemberData(nameof(Connections))]
        public void IfDatabaseExists(IConnectionManager connection)
        {
            if (connection.GetType() != typeof(SQLiteConnectionManager))
            {
                //Arrange
                string dbName = ("ETLBox_" + HashHelper.RandomString(10)).ToLower();
                var existsBefore = IfDatabaseExistsTask.IsExisting(connection, dbName);

                //Act
                SqlTask.ExecuteNonQuery(connection, "Create DB", $"CREATE DATABASE {dbName}");
                var existsAfter = IfDatabaseExistsTask.IsExisting(connection, dbName);

                //Assert
                Assert.False(existsBefore);
                Assert.True(existsAfter);

                //Cleanup
                DropDatabaseTask.Drop(connection, dbName);
            }
        }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => CreateDatabaseTask.Create(Config.SQLiteConnection.ConnectionManager("ControlFlow"), "Test")
                );
        }
    }
}
