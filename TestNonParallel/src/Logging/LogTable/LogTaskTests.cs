using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Definitions.Logging;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using ALE.ETLBox.src.Toolbox.Logging;
using ALE.ETLBoxTests.NonParallel.src;
using ALE.ETLBoxTests.NonParallel.src.Fixtures;
using EtlBox.Logging.Database;
using Microsoft.Extensions.Logging;

namespace ALE.ETLBoxTests.NonParallel.src.Logging.LogTable
{
    public sealed class LogTaskTests : NonParallelTestBase, IDisposable
    {
        public LogTaskTests(LoggingDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        public void Dispose()
        {
            ETLBox.src.Toolbox.ControlFlow.ControlFlow.ClearSettings();
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
            DatabaseLoggingConfiguration.AddDatabaseLoggingConfiguration(
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
                    ETLBox.src.Toolbox.ControlFlow.ControlFlow.LogTable,
                    "message = 'Error!' AND level = 'Error' and task_action = 'LOG'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    ETLBox.src.Toolbox.ControlFlow.ControlFlow.LogTable,
                    "message = 'Warn!' AND level = 'Warn' and task_action = 'LOG'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    ETLBox.src.Toolbox.ControlFlow.ControlFlow.LogTable,
                    "message = 'Info!' AND level = 'Info' and task_action = 'LOG'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    ETLBox.src.Toolbox.ControlFlow.ControlFlow.LogTable,
                    "message = 'Debug!' AND level = 'Debug' and task_action = 'LOG'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    ETLBox.src.Toolbox.ControlFlow.ControlFlow.LogTable,
                    "message = 'Trace!' AND level = 'Trace' and task_action = 'LOG'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    ETLBox.src.Toolbox.ControlFlow.ControlFlow.LogTable,
                    "message = 'Fatal!' AND level = 'Fatal' and task_action = 'LOG'"
                )
            );

            //Cleanup
            DropTableTask.Drop(connection, ETLBox.src.Toolbox.ControlFlow.ControlFlow.LogTable);
        }

        [Fact]
        public void IsControlFlowStageLogged()
        {
            //Arrange
            CreateLogTableTask.Create(SqlConnection, "test_log_stage");
            DatabaseLoggingConfiguration.AddDatabaseLoggingConfiguration(
                SqlConnection,
                LogLevel.Debug,
                "test_log_stage"
            );

            //Act
            ETLBox.src.Toolbox.ControlFlow.ControlFlow.Stage = "SETUP";
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
            DropTableTask.Drop(SqlConnection, ETLBox.src.Toolbox.ControlFlow.ControlFlow.LogTable);
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void TestReadLogTask(IConnectionManager connection)
        {
            //Arrange
            CreateLogTableTask.Create(connection, "test_readlog");
            DatabaseLoggingConfiguration.AddDatabaseLoggingConfiguration(
                connection,
                LogLevel.Information,
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
            DropTableTask.Drop(connection, ETLBox.src.Toolbox.ControlFlow.ControlFlow.LogTable);
        }
    }
}
