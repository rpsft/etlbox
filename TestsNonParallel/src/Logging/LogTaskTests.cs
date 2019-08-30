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
    public class LogTaskTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("Logging");
        public LogTaskTests(LoggingDatabaseFixture dbFixture)
        {
            CreateLogTablesTask.CreateLog(Connection);
        }

        public void Dispose()
        {
            RemoveLogTablesTask.Remove(Connection);
        }

        [Fact]
        public void TestErrorLogging()
        {
            //Arrange
            //Act
            LogTask.Error(Connection, "Error");
            LogTask.Warn(Connection, "Warn");
            LogTask.Info(Connection, "Info");
            //Assert
            Assert.Equal(3, RowCountTask.Count(Connection, "etl.Log",
                "Message in ('Error','Warn','Info')"));
        }
    }
}
