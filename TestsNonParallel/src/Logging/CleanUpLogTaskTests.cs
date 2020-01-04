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
    public class CleanupLogTaskTests : IDisposable
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("Logging");

        public CleanupLogTaskTests(LoggingDatabaseFixture dbFixture)
        {

        }

        public void Dispose()
        {
            ControlFlow.ClearSettings();
        }

        [Theory, MemberData(nameof(Connections))]
        public void CompleteCleanup(IConnectionManager connection)
        {
            //Arrange
            CreateLogTableTask.Create(connection, "test_cleanup_log");
            ControlFlow.AddLoggingDatabaseToConfig(connection, NLog.LogLevel.Trace, "test_cleanup_log");
            //Arrange
            LogTask.Error("Error");
            LogTask.Warn("Warn");
            LogTask.Info("Info");
            //Act
            CleanUpLogTask.CleanUp(connection, 0);
            //Assert
            Assert.Equal(0, new RowCountTask("test_cleanup_log")
            {
                DisableLogging = true,
                ConnectionManager = connection
            }.Count().Rows);

            //Cleanup
            DropTableTask.Drop(connection, ControlFlow.LogTable);
        }
    }
}
