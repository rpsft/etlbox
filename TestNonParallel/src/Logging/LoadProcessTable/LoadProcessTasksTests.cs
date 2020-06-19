using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.Logging;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace ETLBoxTests.Logging
{
    [Collection("Logging")]
    public class LoadProcessTasksTests : IDisposable
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("Logging");
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("Logging");

        public LoadProcessTasksTests(LoggingDatabaseFixture dbFixture)
        {

        }

        public void Dispose()
        {
            ControlFlow.ClearSettings();
        }


        [Theory, MemberData(nameof(Connections))]
        public void CreateLoadProcessTable(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateLoadProcessTableTask.Create(connection, "etlbox_testloadprocess");

            //Assert
            IfTableOrViewExistsTask.IsExisting(connection, "etlbox_testloadprocess");
            var td = TableDefinition.FromTableName(connection, "etlbox_testloadprocess");
            Assert.True(td.Columns.Count == 11);

            //Cleanup
            DropTableTask.Drop(connection, "etlbox_testloadprocess");
        }

        [Theory, MemberData(nameof(Connections))]
        public void StartLoadProcess(IConnectionManager conn)
        {
            //Arrange
            CreateLoadProcessTableTask.Create(conn, "test_load_process");

            //Act
            StartLoadProcessTask.Start(conn, "Test process 1");

            //Assert
            Assert.True(ControlFlow.CurrentLoadProcess != null);
            Assert.Equal("Test process 1", ControlFlow.CurrentLoadProcess.ProcessName);
            Assert.True(ControlFlow.CurrentLoadProcess.StartDate >= DateTime.Now.AddSeconds(-1));
            Assert.Equal(1, RowCountTask.Count(conn, ControlFlow.LoadProcessTable,
                $"{conn.QB}start_message{conn.QE} IS NULL and {conn.QB}end_message{conn.QE} IS NULL and {conn.QB}abort_message{conn.QE} IS NULL"));
            Assert.Equal(1, RowCountTask.Count(conn, ControlFlow.LoadProcessTable,
                $"{conn.QB}is_running{conn.QE} = 1 AND {conn.QB}was_successful{conn.QE}=0 AND {conn.QB}was_aborted{conn.QE}=0"));

            //Cleanup
            DropTableTask.Drop(conn, "test_load_process");
        }

        [Theory, MemberData(nameof(Connections))]
        public void StartLoadProcessWithMessage(IConnectionManager conn)
        {
            //Arrange
            CreateLoadProcessTableTask.Create(conn, "test_lp_withmessage");

            //Act
            StartLoadProcessTask.Start(conn, "Test process 1", "Message 1", "SourceA");

            //Assert
            Assert.Equal(1, RowCountTask.Count(conn, "test_lp_withmessage",
                $"{conn.QB}start_message{conn.QE} = 'Message 1' AND {conn.QB}source{conn.QE}='SourceA' AND {conn.QB}end_message{conn.QE} IS NULL AND {conn.QB}abort_message{conn.QE} IS NULL"));

            //Cleanup
            DropTableTask.Drop(conn, "test_lp_withmessage");
        }

        [Theory, MemberData(nameof(Connections))]
        public void EndLoadProcess(IConnectionManager conn)
        {
            //Arrange
            CreateLoadProcessTableTask.Create(conn, "test_lp_end");

            StartLoadProcessTask.Start(conn, "Test process 2");
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == true);

            //Act
            EndLoadProcessTask.End(conn, "End process 2");

            //Assert
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == false);
            Assert.True(ControlFlow.CurrentLoadProcess.WasSuccessful == true);
            Assert.True(ControlFlow.CurrentLoadProcess.IsFinished == true);
            Assert.True(ControlFlow.CurrentLoadProcess.EndDate >= DateTime.Now.AddSeconds(-1));

            Assert.Equal(1, RowCountTask.Count(conn, "test_lp_end",
                $"{conn.QB}is_running{conn.QE}=0 and {conn.QB}was_successful{conn.QE}=1 and {conn.QB}was_aborted{conn.QE}=0"));
            Assert.Equal(1, RowCountTask.Count(conn, "test_lp_end",
                 $"{conn.QB}start_message{conn.QE} IS NULL AND {conn.QB}end_message{conn.QE} = 'End process 2' AND {conn.QB}abort_message{conn.QE} IS NULL"));

            //Cleanup
            DropTableTask.Drop(conn, "test_lp_end");
        }

        [Theory, MemberData(nameof(Connections))]
        public void AbortLoadProcess(IConnectionManager connection)
        {
            //Arrange
            var conn = SqlConnection;
            CreateLoadProcessTableTask.Create(SqlConnection, "test_lp_abort");

            StartLoadProcessTask.Start(SqlConnection, "Test process 3");
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == true);

            //Act
            AbortLoadProcessTask.Abort(SqlConnection, ControlFlow.CurrentLoadProcess.Id);

            //Assert
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == false);
            Assert.True(ControlFlow.CurrentLoadProcess.WasAborted == true);
            Assert.True(ControlFlow.CurrentLoadProcess.AbortMessage == null);
            Assert.Equal(1, RowCountTask.Count(SqlConnection, "test_lp_abort"
                , $"{conn.QB}is_running{conn.QE}=0 and {conn.QB}was_successful{conn.QE}=0 and {conn.QB}was_aborted{conn.QE}=1"));

            //Cleanup
            DropTableTask.Drop(SqlConnection, "test_lp_abort");
        }

        [Theory, MemberData(nameof(Connections))]
        public void IsLoadProcessKeyInLog(IConnectionManager conn)
        {
            //Arrange
            CreateLoadProcessTableTask.Create(conn, "test_lpkey_inlog");
            CreateLogTableTask.Create(conn, "test_lpkey_log");
            ControlFlow.AddLoggingDatabaseToConfig(conn, NLog.LogLevel.Info, "test_lpkey_log");
            StartLoadProcessTask.Start(conn, "Test process 5");

            //Act
            string fromdual = conn.GetType() == typeof(OracleConnectionManager) ? "FROM DUAL" : "";
            SqlTask.ExecuteNonQuery(conn, "Test Task", $"Select 1 AS test {fromdual}");

            //Assert
            Assert.Equal(2, new RowCountTask("test_lpkey_log",
                $"{conn.QB}message{conn.QE}='Test Task' AND {conn.QB}load_process_id{conn.QE} = {ControlFlow.CurrentLoadProcess.Id}")
            {
                DisableLogging = true,
                ConnectionManager = conn
            }.Count().Rows); ;

            //Cleanup
            DropTableTask.Drop(conn, ControlFlow.LogTable);
            DropTableTask.Drop(conn, ControlFlow.LoadProcessTable);
        }



        [Fact]
        public void ReadLastSuccessfulProcess()
        {
            //Arrange
            ControlFlow.DefaultDbConnection = SqlConnection;
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
            ControlFlow.DefaultDbConnection = SqlConnection;
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
            ControlFlow.DefaultDbConnection = SqlConnection;
            CreateLoadProcessTableTask.Create("test_lp_restart");
            CreateLogTableTask.Create("test_log_restart");
            ControlFlow.AddLoggingDatabaseToConfig(SqlConnection, NLog.LogLevel.Info, "test_log_restart");

            //Act
            StartLoadProcessTask.Start("Test process 13");
            long? processId1 = ControlFlow.CurrentLoadProcess.Id;
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.Equal(2, new RowCountTask("test_log_restart", $"message='Test Task' AND load_process_id = {processId1}")
            { DisableLogging = true }.Count().Rows);

            StartLoadProcessTask.Start("Test process 14");
            long? processId2 = ControlFlow.CurrentLoadProcess.Id;

            //Assert
            Assert.NotEqual(processId1, processId2);
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.Equal(2, new RowCountTask("test_log_restart", $"message='Test Task' AND load_process_id = {processId2}")
            { DisableLogging = true }.Count().Rows);

            //Cleanup
            DropTableTask.Drop(ControlFlow.LogTable);
            DropTableTask.Drop(ControlFlow.LoadProcessTable);
        }

        [Fact]
        public void IsLoadProcessKeySetForLogTask()
        {
            //Arrange
            ControlFlow.DefaultDbConnection = SqlConnection;
            CreateLoadProcessTableTask.Create("test_lp_logtask");
            CreateLogTableTask.Create("test_log_logtask");
            ControlFlow.AddLoggingDatabaseToConfig(SqlConnection, NLog.LogLevel.Info, "test_log_logtask");

            //Act
            StartLoadProcessTask.Start("Test process 15");
            long? processId1 = ControlFlow.CurrentLoadProcess.Id;
            LogTask.Error("Test1");
            LogTask.Warn("Test2");
            LogTask.Info("Test3");
            //Assert
            Assert.Equal(3, new RowCountTask("test_log_logtask",
                $"message like 'Test%' AND load_process_id = {processId1}")
            { DisableLogging = true }.Count().Rows);

            //Cleanup
            DropTableTask.Drop(ControlFlow.LogTable);
            DropTableTask.Drop(ControlFlow.LoadProcessTable);
        }
    }
}
