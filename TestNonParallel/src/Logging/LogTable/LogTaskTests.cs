using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.Logging;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.Logging
{
    [Collection("Logging")]
    public class LogTaskTests : IDisposable
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("Logging");
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("Logging");

        public LogTaskTests(LoggingDatabaseFixture dbFixture)
        {

        }

        public void Dispose()
        {
            ControlFlow.ClearSettings();
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateLogTable(IConnectionManager connection)
        {
            //Arrange
            //Act
            CreateLogTableTask.Create(connection, "etlbox_testlog");

            //Assert
            IfTableOrViewExistsTask.IsExisting(connection, "etlbox_testlog");
            var td = TableDefinition.FromTableName(connection, "etlbox_testlog");
            Assert.True(td.Columns.Count == 10);
            //Cleanup
            DropTableTask.Drop(connection, "etlbox_testlog");
        }


        [Theory, MemberData(nameof(Connections))]
        public void TestErrorLogging(IConnectionManager conn)
        {
            //Arrange
            CreateLogTableTask.Create(conn, "test_log");
            ControlFlow.AddLoggingDatabaseToConfig(conn, NLog.LogLevel.Trace, "test_log");
            //Act
            LogTask.Error(conn, "Error!");
            LogTask.Warn(conn, "Warn!");
            LogTask.Info(conn, "Info!");
            LogTask.Debug(conn, "Debug!");
            LogTask.Trace(conn, "Trace!");
            LogTask.Fatal(conn, "Fatal!");
            //Assert
            Assert.Equal(1, RowCountTask.Count(conn, ControlFlow.LogTable,
                $"{conn.QB}message{conn.QE} = 'Error!' AND {conn.QB}level{conn.QE} = 'Error' AND {conn.QB}task_action{conn.QE} = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(conn, ControlFlow.LogTable,
                $"{conn.QB}message{conn.QE} = 'Warn!' AND {conn.QB}level{conn.QE} = 'Warn' AND {conn.QB}task_action{conn.QE} = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(conn, ControlFlow.LogTable,
                $"{conn.QB}message{conn.QE} = 'Info!' AND {conn.QB}level{conn.QE} = 'Info' AND {conn.QB}task_action{conn.QE} = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(conn, ControlFlow.LogTable,
                $"{conn.QB}message{conn.QE} = 'Debug!' AND {conn.QB}level{conn.QE} = 'Debug' AND {conn.QB}task_action{conn.QE} = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(conn, ControlFlow.LogTable,
                $"{conn.QB}message{conn.QE} = 'Trace!' AND {conn.QB}level{conn.QE} = 'Trace' AND {conn.QB}task_action{conn.QE} = 'LOG'"));
            Assert.Equal(1, RowCountTask.Count(conn, ControlFlow.LogTable,
                $"{conn.QB}message{conn.QE} = 'Fatal!' AND {conn.QB}level{conn.QE} = 'Fatal' AND {conn.QB}task_action{conn.QE} = 'LOG'"));

            //Cleanup
            DropTableTask.Drop(conn, ControlFlow.LogTable);
        }

        [Fact]
        public void IsControlFlowStageLogged()
        {
            //Arrange
            CreateLogTableTask.Create(SqlConnection, "test_log_stage");
            ControlFlow.AddLoggingDatabaseToConfig(SqlConnection, NLog.LogLevel.Debug, "test_log_stage");

            //Act
            ControlFlow.STAGE = "SETUP";
            SqlTask.ExecuteNonQuery(SqlConnection, "Test Task", "Select 1 as test");

            //Assert
            Assert.Equal(2, new RowCountTask("test_log_stage",
                           $"message='Test Task' AND stage = 'SETUP'")
            {
                DisableLogging = true,
                ConnectionManager = SqlConnection
            }.Count().Rows);

            //Cleanup
            DropTableTask.Drop(SqlConnection, ControlFlow.LogTable);
        }

        [Theory, MemberData(nameof(Connections))]
        public void TestReadLogTask(IConnectionManager connection)
        {
            //Arrange
            CreateLogTableTask.Create(connection, "test_readlog");
            ControlFlow.AddLoggingDatabaseToConfig(connection, NLog.LogLevel.Info, "test_readlog");
            string fromdual = connection.GetType() == typeof(OracleConnectionManager) ? " FROM DUAL" : "";
            SqlTask.ExecuteNonQuery(connection, "Test Task", $"Select 1 as test {fromdual}");

            //Act
            List<LogEntry> entries = ReadLogTableTask.Read(connection);

            //Assert
            Assert.Collection<LogEntry>(entries,
                 l => Assert.True(l.Message == "Test Task" && l.TaskAction == "START" && l.TaskType == "SqlTask"),
                 l => Assert.True(l.Message == "Test Task" && l.TaskAction == "END" && l.TaskType == "SqlTask")
                 );

            //Cleanup
            DropTableTask.Drop(connection, ControlFlow.LogTable);
        }
    }
}
