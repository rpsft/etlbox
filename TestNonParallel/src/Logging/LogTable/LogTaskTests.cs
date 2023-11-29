using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.NonParallel.Fixtures;
using ETLBox.Primitives;
using NLog;

namespace ALE.ETLBoxTests.NonParallel.Logging.LogTable
{
    public sealed class LogTaskTests : NonParallelTestBase, IDisposable
    {
        public LogTaskTests(LoggingDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        public void Dispose()
        {
            ETLBox.Common.ControlFlow.ControlFlow.ClearSettings();
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void CreateLogTable(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateLogTableTask.Create(connection, "etlbox_testlog");

            //Assert
            IfTableOrViewExistsTask.IsExisting(connection, "etlbox_testlog");
            var td = TableDefinition.GetDefinitionFromTableName(connection, "etlbox_testlog");
            Assert.True(td.Columns.Count == 10);
            //Cleanup
            DropTableTask.Drop(connection, "etlbox_testlog");
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void TestErrorLogging(IConnectionManager connection)
        {
            //Arrange
            CreateLogTableTask.Create(connection, "test_log");
            ETLBox.Common.ControlFlow.ControlFlow.AddLoggingDatabaseToConfig(
                connection,
                LogLevel.Trace,
                "test_log"
            );
            //Act
            LogTask.Error(connection, "Error!");
            LogTask.Warn(connection, "Warn!");
            LogTask.Info(connection, "Info!");
            LogTask.Debug(connection, "Debug!");
            LogTask.Trace(connection, "Trace!");
            LogTask.Fatal(connection, "Fatal!");
            //Assert
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    ETLBox.Common.ControlFlow.ControlFlow.LogTable,
                    "message = 'Error!' AND level = 'Error' and task_action = 'LOG'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    ETLBox.Common.ControlFlow.ControlFlow.LogTable,
                    "message = 'Warn!' AND level = 'Warn' and task_action = 'LOG'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    ETLBox.Common.ControlFlow.ControlFlow.LogTable,
                    "message = 'Info!' AND level = 'Info' and task_action = 'LOG'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    ETLBox.Common.ControlFlow.ControlFlow.LogTable,
                    "message = 'Debug!' AND level = 'Debug' and task_action = 'LOG'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    ETLBox.Common.ControlFlow.ControlFlow.LogTable,
                    "message = 'Trace!' AND level = 'Trace' and task_action = 'LOG'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    ETLBox.Common.ControlFlow.ControlFlow.LogTable,
                    "message = 'Fatal!' AND level = 'Fatal' and task_action = 'LOG'"
                )
            );

            //Cleanup
            DropTableTask.Drop(connection, ETLBox.Common.ControlFlow.ControlFlow.LogTable);
        }

        [Fact]
        public void IsControlFlowStageLogged()
        {
            //Arrange
            CreateLogTableTask.Create(SqlConnection, "test_log_stage");
            ETLBox.Common.ControlFlow.ControlFlow.AddLoggingDatabaseToConfig(
                SqlConnection,
                LogLevel.Debug,
                "test_log_stage"
            );

            //Act
            ETLBox.Common.ControlFlow.ControlFlow.Stage = "SETUP";
            SqlTask.ExecuteNonQuery(SqlConnection, "Test Task", "Select 1 as test");

            //Assert
            Assert.Equal(
                2,
                new RowCountTask("test_log_stage", "message='Test Task' and stage = 'SETUP'")
                {
                    DisableLogging = true,
                    ConnectionManager = SqlConnection
                }
                    .Count()
                    .Rows
            );

            //Cleanup
            DropTableTask.Drop(SqlConnection, ETLBox.Common.ControlFlow.ControlFlow.LogTable);
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void TestReadLogTask(IConnectionManager connection)
        {
            //Arrange
            CreateLogTableTask.Create(connection, "test_readlog");
            ETLBox.Common.ControlFlow.ControlFlow.AddLoggingDatabaseToConfig(
                connection,
                LogLevel.Info,
                "test_readlog"
            );
            SqlTask.ExecuteNonQuery(connection, "Test Task", "Select 1 as test");

            //Act
            List<LogEntry> entries = ReadLogTableTask.Read(connection);

            //Assert
            Assert.Collection(
                entries,
                l =>
                    Assert.True(
                        l.Message == "Test Task"
                            && l.TaskAction == "START"
                            && l.TaskType == "SqlTask"
                    ),
                l =>
                    Assert.True(
                        l.Message == "Test Task" && l.TaskAction == "END" && l.TaskType == "SqlTask"
                    )
            );

            //Cleanup
            DropTableTask.Drop(connection, ETLBox.Common.ControlFlow.ControlFlow.LogTable);
        }
    }
}
