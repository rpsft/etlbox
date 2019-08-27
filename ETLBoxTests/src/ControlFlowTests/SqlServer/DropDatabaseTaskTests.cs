using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests.SqlServer
{
    [Collection("Sql Server ControlFlow")]
    public class DropDatabaseTaskTests
    {
        public SqlConnectionManager MasterConnection => new SqlConnectionManager(Config.SqlConnectionString("ControlFlow").GetMasterConnection());
        public DropDatabaseTaskTests()
        { }

        [Fact]
        public void Drop()
        {
            //Arrange
            string dbName = "ETLBox_"+HashHelper.RandomString(10);
            var sqlTask = new SqlTask("Get assert data", $"select cast(db_id('{dbName}') as int)")
            {
                ConnectionManager = MasterConnection
            };
            CreateDatabaseTask.Create(MasterConnection, dbName);
            Assert.True(sqlTask.ExecuteScalarAsBool());
            //Act
            DropDatabaseTask.Drop(MasterConnection, dbName);
            //Assert
            Assert.False(sqlTask.ExecuteScalarAsBool());
        }

    }
}
