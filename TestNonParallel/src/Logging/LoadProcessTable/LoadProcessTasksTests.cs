﻿using System.Threading.Tasks;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.NonParallel.Fixtures;
using NLog;

namespace ALE.ETLBoxTests.NonParallel.Logging.LoadProcessTable
{
    public sealed class LoadProcessTasksTests : NonParallelTestBase, IDisposable
    {
        public LoadProcessTasksTests(LoggingDatabaseFixture fixture)
            : base(fixture) { }

        public void Dispose()
        {
            ETLBox.ControlFlow.ControlFlow.ClearSettings();
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void CreateLoadProcessTable(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateLoadProcessTableTask.Create(connection, "etlbox_testloadprocess");

            //Assert
            IfTableOrViewExistsTask.IsExisting(connection, "etlbox_testloadprocess");
            var td = TableDefinition.GetDefinitionFromTableName(
                connection,
                "etlbox_testloadprocess"
            );
            Assert.True(td.Columns.Count == 11);

            //Cleanup
            DropTableTask.Drop(connection, "etlbox_testloadprocess");
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void StartLoadProcess(IConnectionManager connection)
        {
            //Arrange
            CreateLoadProcessTableTask.Create(connection, "test_load_process");

            //Act
            StartLoadProcessTask.Start(connection, "Test process 1");

            //Assert
            Assert.True(ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess != null);
            Assert.Equal(
                "Test process 1",
                ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.ProcessName
            );
            Assert.True(
                ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.StartDate
                    >= DateTime.Now.AddSeconds(-1)
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    ETLBox.ControlFlow.ControlFlow.LoadProcessTable,
                    "start_message IS NULL and end_message IS NULL and abort_message IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    ETLBox.ControlFlow.ControlFlow.LoadProcessTable,
                    "is_running = 1 AND was_successful=0 AND was_aborted=0"
                )
            );

            //Cleanup
            DropTableTask.Drop(connection, "test_load_process");
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void StartLoadProcessWithMessage(IConnectionManager connection)
        {
            //Arrange
            CreateLoadProcessTableTask.Create(connection, "test_lp_withmessage");

            //Act
            StartLoadProcessTask.Start(connection, "Test process 1", "Message 1", "SourceA");

            //Assert
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "test_lp_withmessage",
                    "start_message = 'Message 1' AND source='SourceA' AND end_message IS NULL AND abort_message IS NULL"
                )
            );

            //Cleanup
            DropTableTask.Drop(connection, "test_lp_withmessage");
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void EndLoadProcess(IConnectionManager connection)
        {
            //Arrange
            CreateLoadProcessTableTask.Create(connection, "test_lp_end");

            StartLoadProcessTask.Start(connection, "Test process 2");
            Assert.True(ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.IsRunning);

            //Act
            EndLoadProcessTask.End(connection, "End process 2");

            //Assert
            Assert.False(ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.IsRunning);
            Assert.True(ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.WasSuccessful);
            Assert.True(ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.IsFinished);
            Assert.True(
                ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.EndDate
                    >= DateTime.Now.AddSeconds(-1)
            );

            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "test_lp_end",
                    "is_running=0 and was_successful=1 and was_aborted=0"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "test_lp_end",
                    "start_message IS NULL AND end_message = 'End process 2' AND abort_message IS NULL"
                )
            );

            //Cleanup
            DropTableTask.Drop(connection, "test_lp_end");
        }

        [Fact]
        public void AbortLoadProcess()
        {
            //Arrange
            CreateLoadProcessTableTask.Create(SqlConnection, "test_lp_abort");

            StartLoadProcessTask.Start(SqlConnection, "Test process 3");
            Assert.True(ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.IsRunning);

            //Act
            AbortLoadProcessTask.Abort(
                SqlConnection,
                ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.Id
            );

            //Assert
            Assert.False(ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.IsRunning);
            Assert.True(ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.WasAborted);
            Assert.True(ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.AbortMessage == null);
            Assert.Equal(
                1,
                RowCountTask.Count(
                    SqlConnection,
                    "test_lp_abort",
                    "is_running=0 and was_successful=0 and was_aborted=1"
                )
            );

            //Cleanup
            DropTableTask.Drop(SqlConnection, "test_lp_abort");
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void IsLoadProcessKeyInLog(IConnectionManager connection)
        {
            //Arrange
            CreateLoadProcessTableTask.Create(connection, "test_lpkey_inlog");
            CreateLogTableTask.Create(connection, "test_lpkey_log");
            ETLBox.ControlFlow.ControlFlow.AddLoggingDatabaseToConfig(
                connection,
                LogLevel.Info,
                "test_lpkey_log"
            );
            StartLoadProcessTask.Start(connection, "Test process 5");

            //Act
            SqlTask.ExecuteNonQuery(connection, "Test Task", "Select 1 as test");

            //Assert
            Assert.Equal(
                2,
                new RowCountTask(
                    "test_lpkey_log",
                    $"message='Test Task' and load_process_id = {ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.Id}"
                )
                {
                    DisableLogging = true,
                    ConnectionManager = connection
                }
                    .Count()
                    .Rows
            );

            //Cleanup
            DropTableTask.Drop(connection, ETLBox.ControlFlow.ControlFlow.LogTable);
            DropTableTask.Drop(connection, ETLBox.ControlFlow.ControlFlow.LoadProcessTable);
        }

        [Fact]
        public void ReadLastSuccessfulProcess()
        {
            //Arrange
            ETLBox.ControlFlow.ControlFlow.DefaultDbConnection = SqlConnection;
            CreateLoadProcessTableTask.Create("test_lpkey_lastsuccess");
            StartLoadProcessTask.Start("Test process 8");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            EndLoadProcessTask.End();
            Task.Delay(10).Wait();
            StartLoadProcessTask.Start("Test process 9");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            EndLoadProcessTask.End();

            //Act
            var lp = ReadLoadProcessTableTask.ReadWithOption(ReadOptions.ReadLastSuccessful);

            //Assert
            Assert.True(lp.IsFinished);
            Assert.True(lp.WasSuccessful);
            Assert.False(lp.WasAborted);
            Assert.Equal("Test process 9", lp.ProcessName);

            //Cleanup
            DropTableTask.Drop("test_lpkey_lastsuccess");
        }

        [Fact]
        public void ReadLastAbortedProcess()
        {
            //Arrange
            ETLBox.ControlFlow.ControlFlow.DefaultDbConnection = SqlConnection;
            CreateLoadProcessTableTask.Create("test_lpkey_lastabort");
            StartLoadProcessTask.Start("Test process 10");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            EndLoadProcessTask.End();
            Task.Delay(10).Wait();
            StartLoadProcessTask.Start("Test process 11");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            AbortLoadProcessTask.Abort();
            StartLoadProcessTask.Start("Test process 12");
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact
            EndLoadProcessTask.End();

            //Act
            var lp = ReadLoadProcessTableTask.ReadWithOption(ReadOptions.ReadLastAborted);

            //Assert
            Assert.True(lp.IsFinished);
            Assert.True(lp.WasAborted);
            Assert.False(lp.WasSuccessful);
            Assert.Equal("Test process 11", lp.ProcessName);

            //Cleanup
            DropTableTask.Drop(SqlConnection, "test_lpkey_lastabort");
        }

        [Fact]
        public void IsLoadProcessKeySetIfRestarted()
        {
            //Arrange
            ETLBox.ControlFlow.ControlFlow.DefaultDbConnection = SqlConnection;
            CreateLoadProcessTableTask.Create("test_lp_restart");
            CreateLogTableTask.Create("test_log_restart");
            ETLBox.ControlFlow.ControlFlow.AddLoggingDatabaseToConfig(
                SqlConnection,
                LogLevel.Info,
                "test_log_restart"
            );

            //Act
            StartLoadProcessTask.Start("Test process 13");
            long? processId1 = ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.Id;
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.Equal(
                2,
                new RowCountTask(
                    "test_log_restart",
                    $"message='Test Task' AND load_process_id = {processId1}"
                )
                {
                    DisableLogging = true
                }
                    .Count()
                    .Rows
            );

            StartLoadProcessTask.Start("Test process 14");
            long? processId2 = ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.Id;

            //Assert
            Assert.NotEqual(processId1, processId2);
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.Equal(
                2,
                new RowCountTask(
                    "test_log_restart",
                    $"message='Test Task' AND load_process_id = {processId2}"
                )
                {
                    DisableLogging = true
                }
                    .Count()
                    .Rows
            );

            //Cleanup
            DropTableTask.Drop(ETLBox.ControlFlow.ControlFlow.LogTable);
            DropTableTask.Drop(ETLBox.ControlFlow.ControlFlow.LoadProcessTable);
        }

        [Fact]
        public void IsLoadProcessKeySetForLogTask()
        {
            //Arrange
            ETLBox.ControlFlow.ControlFlow.DefaultDbConnection = SqlConnection;
            CreateLoadProcessTableTask.Create("test_lp_logtask");
            CreateLogTableTask.Create("test_log_logtask");
            ETLBox.ControlFlow.ControlFlow.AddLoggingDatabaseToConfig(
                SqlConnection,
                LogLevel.Info,
                "test_log_logtask"
            );

            //Act
            StartLoadProcessTask.Start("Test process 15");
            long? processId1 = ETLBox.ControlFlow.ControlFlow.CurrentLoadProcess.Id;
            LogTask.Error("Test1");
            LogTask.Warn("Test2");
            LogTask.Info("Test3");
            //Assert
            Assert.Equal(
                3,
                new RowCountTask(
                    "test_log_logtask",
                    $"message like 'Test%' AND load_process_id = {processId1}"
                )
                {
                    DisableLogging = true
                }
                    .Count()
                    .Rows
            );

            //Cleanup
            DropTableTask.Drop(ETLBox.ControlFlow.ControlFlow.LogTable);
            DropTableTask.Drop(ETLBox.ControlFlow.ControlFlow.LoadProcessTable);
        }
    }
}