using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.Logging
{
    [Collection("Logging")]
    public class RemoveLogTablesTaskTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("Logging");
        public RemoveLogTablesTaskTests(LoggingDatabaseFixture dbFixture)
        {

        }

        public void Dispose()
        {

        }

        [Fact]
        public void RemoveLogTables()
        {
            //Arrange
            RemoveLogTablesTask.Remove(Connection);
            CreateLogTablesTask.CreateLog(Connection, "Log");
            //Act
            RemoveLogTablesTask.Remove(Connection);
            //Assert
            Assert.True(SqlTask.ExecuteScalarAsBool(Connection, "Check if tables are deleted",
                "SELECT CASE WHEN object_id('etl.LoadProcess') IS NULL THEN 1 ELSE 0 END"));
            Assert.True(SqlTask.ExecuteScalarAsBool(Connection, "Check if tables are deleted",
                "SELECT CASE WHEN object_id('etl.Log') IS NULL THEN 1 ELSE 0 END"));
        }
    }
}
