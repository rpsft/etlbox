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
    public class LogTaskTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("Logging");
        public LogTaskTests(LoggingDatabaseFixture dbFixture)
        {

        }


        [Theory, MemberData(nameof(Connections))]
        public void TestErrorLogging(IConnectionManager connection)
        {
            //Arrange
            CreateLogTableTask.Create(connection, "test_log");
            ControlFlow.SetLoggingDatabase(connection,NLog.LogLevel.Trace, "test_log");
            //Act
            LogTask.Error(connection, "Error!");
            LogTask.Warn(connection, "Warn!");
            LogTask.Info(connection, "Info!");
            LogTask.Debug(connection, "Debug!");
            LogTask.Trace(connection, "Trace!");
            LogTask.Fatal(connection, "Fatal!");
            //Assert
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.CurrentLogTable,
                "message = 'Error!' AND level = 'Error' and task_action = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.CurrentLogTable,
                "message = 'Warn!' AND level = 'Warn' and task_action = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.CurrentLogTable,
                "message = 'Info!' AND level = 'Info' and task_action = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.CurrentLogTable,
                "message = 'Debug!' AND level = 'Debug' and task_action = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.CurrentLogTable,
                "message = 'Trace!' AND level = 'Trace' and task_action = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(connection, ControlFlow.CurrentLogTable,
                "message = 'Fatal!' AND level = 'Fatal' and task_action = 'LOG'"));

            DropTableTask.Drop(connection, ControlFlow.CurrentLogTable);
        }
    }
}
