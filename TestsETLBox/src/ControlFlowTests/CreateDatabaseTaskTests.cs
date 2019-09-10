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
        public CreateDatabaseTaskTests()
        { }

        [Fact]
        public void CreateWithAllParameters()
        {
            //Arrange
            string dbName = "ETLBox_"+HashHelper.RandomString(10);
            var sqlTask = new SqlTask("Get assert data", $"select cast(db_id('{dbName}') as int)")
            {
                ConnectionManager = MasterConnection
            };
            Assert.False(sqlTask.ExecuteScalarAsBool());

            //Act
            CreateDatabaseTask.Create(MasterConnection, dbName);

            //Assert
            Assert.True(sqlTask.ExecuteScalarAsBool());

            //Cleanup
            DropDatabaseTask.Drop(MasterConnection, dbName);
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
