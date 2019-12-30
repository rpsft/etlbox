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
    public class CleanUpLogTaskTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("Logging");
        public CleanUpLogTaskTests(LoggingDatabaseFixture dbFixture)
        {
            CreateLogTablesTask.CreateLog(Connection, "Log");
        }

        public void Dispose()
        {
            RemoveLogTablesTask.Remove(Connection);
        }

        [Fact]
        public void LogCleanup()
        {
            //Arrange
            LogTask.Error("Error");
            LogTask.Warn("Warn");
            LogTask.Info("Info");
            //Act
            CleanUpLogTask.CleanUp(Connection, 0);
            //Assert
            Assert.Equal(0, new RowCountTask("etl.Log ") {
                DisableLogging = true,
                ConnectionManager = Connection
            }.Count().Rows);
        }
    }
}
