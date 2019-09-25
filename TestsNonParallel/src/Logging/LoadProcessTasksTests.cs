using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ALE.ETLBoxTests.Logging
{
    [Collection("Logging")]
    public class LoadProcessTasksTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("Logging");
        public LoadProcessTasksTests(LoggingDatabaseFixture dbFixture)
        {
            CreateLogTablesTask.CreateLog(Connection);
            ControlFlow.CurrentDbConnection = Connection;
        }

        public void Dispose()
        {
            RemoveLogTablesTask.Remove(Connection);
            ControlFlow.ClearSettings();
        }

        [Fact]
        public void StartLoadProcess()
        {
            //Arrange
            DateTime beforeTask = DateTime.Now;
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact

            //Act
            StartLoadProcessTask.Start("Test process 1");

            //Assert
            DateTime afterTask = DateTime.Now;
            Assert.True(ControlFlow.CurrentLoadProcess != null);
            Assert.Equal("Test process 1", ControlFlow.CurrentLoadProcess.ProcessName);
            Assert.True(ControlFlow.CurrentLoadProcess.StartDate <= afterTask && ControlFlow.CurrentLoadProcess.StartDate >= beforeTask);
            Assert.False(new RowCountTask("etl.Log") { DisableLogging = true }.Count().HasRows);
            Assert.Equal(1, new RowCountTask("etl.LoadProcess",
                "StartMessage is null and EndMessage is null and AbortMessage is null")
            {
                DisableLogging = true,
            }.Count().Rows);
            Assert.Equal(1, new RowCountTask("etl.LoadProcess",
                "IsRunning=1 and WasSuccessful=0 and WasAborted=0")
            {
                DisableLogging = true,
            }.Count().Rows);

        }

        [Fact]
        public void StartLoadProcessWithMessage()
        {
            //Arrange
            //Act
            StartLoadProcessTask.Start("Test process 1", "Message 1", "SourceA");
            //Assert
            Assert.Equal(1, new RowCountTask("etl.LoadProcess",
                "StartMessage = 'Message 1' and Source='SourceA' and EndMessage is null and AbortMessage is null")
            {
                DisableLogging = true
            }.Count().Rows);
        }

        [Fact]
        public void EndLoadProcess()
        {
            //Arrange
            StartLoadProcessTask.Start("Test process 2");
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == true);
            DateTime beforeTask = DateTime.Now;
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact

            //Act
            EndLoadProcessTask.End();

            //Assert
            DateTime afterTask = DateTime.Now;
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == false);
            Assert.True(ControlFlow.CurrentLoadProcess.WasSuccessful == true);
            Assert.True(ControlFlow.CurrentLoadProcess.IsFinished == true);
            Assert.True(ControlFlow.CurrentLoadProcess.EndDate <= afterTask && ControlFlow.CurrentLoadProcess.EndDate >= beforeTask);
            Assert.False(new SqlTask("Check if logging was disabled for end process task",
                "select count(*) from etl.Log")
            { DisableLogging = true }.ExecuteScalarAsBool());
            Assert.Equal(1, new RowCountTask("etl.LoadProcess ", "IsRunning=0 and WasSuccessful=1 and WasAborted=0")
            {
                DisableLogging = true
            }.Count().Rows);
            Assert.Equal(1, new RowCountTask("etl.LoadProcess",
                "StartMessage is null and EndMessage is null and AbortMessage is null")
            {
                DisableLogging = true
            }.Count().Rows);
        }

        [Fact]
        public void AbortLoadProcess()
        {
            //Arrange
            StartLoadProcessTask.Start("Test process 3");
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == true);
            DateTime beforeTask = DateTime.Now;
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact

            //Act
            AbortLoadProcessTask.Abort(ControlFlow.CurrentLoadProcess.LoadProcessKey, "AbortMessage");

            //Assert
            DateTime afterTask = DateTime.Now;
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == false);
            Assert.True(ControlFlow.CurrentLoadProcess.WasAborted == true);
            Assert.True(ControlFlow.CurrentLoadProcess.EndDate <= afterTask && ControlFlow.CurrentLoadProcess.EndDate >= beforeTask);
            Assert.True(ControlFlow.CurrentLoadProcess.AbortMessage == "AbortMessage");
            Assert.False(new RowCountTask("etl.Log") { DisableLogging = true }.Count().HasRows);
            Assert.Equal(1, new RowCountTask("etl.LoadProcess", "IsRunning=0 and WasSuccessful=0 and WasAborted=1")
            {
                DisableLogging = true
            }.Count().Rows);

        }

        [Fact]
        public void IsTransferCompletedForLoadProcess()
        {
            //Arrange
            StartLoadProcessTask.Start("Test process 4");
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == true);
            DateTime beforeTask = DateTime.Now;
            Task.Delay(10).Wait(); //Sql Server datetime is not that exact

            //Act
            TransferCompletedForLoadProcessTask.Complete(ControlFlow.CurrentLoadProcess.LoadProcessKey);

            //Assert
            Assert.Equal(2, new RowCountTask("etl.Log", "TaskType='TransferCompletedForLoadProcessTask'")
            {
                DisableLogging = true
            }.Count().Rows);
            DateTime afterTask = DateTime.Now;
            Assert.True(ControlFlow.CurrentLoadProcess.IsRunning == true);
            Assert.True(ControlFlow.CurrentLoadProcess.TransferCompletedDate <= afterTask && ControlFlow.CurrentLoadProcess.TransferCompletedDate >= beforeTask);
        }

        [Fact]
        public void IsLoadProcessKeyInLog()
        {
            //Arrange
            StartLoadProcessTask.Start("Test process 5");

            //Act
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");

            //Assert
            Assert.Equal(2, new RowCountTask("etl.Log",
                $"Message='Test Task' and LoadProcessKey = {ControlFlow.CurrentLoadProcess.LoadProcessKey}")
            {
                DisableLogging = true
            }.Count().Rows);
        }

        [Fact]
        public void IsControlFlowStageLogged()
        {
            //Arrange
            StartLoadProcessTask.Start("Test process 1", "Message 1", "SourceA");

            //Act
            ControlFlow.STAGE = "SETUP";
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            //Assert
            Assert.Equal(2, new RowCountTask("etl.Log",
                           $"Message='Test Task' and Stage = 'SETUP'")
            {
                DisableLogging = true
            }.Count().Rows);
        }

        [Fact]
        public void ReadLastSuccessfulProcess()
        {
            //Arrange
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
            Assert.Equal(2, new RowCountTask("etl.LoadProcess", "IsFinished=1") { DisableLogging = true }.Count().Rows);
            Assert.Equal(2, new RowCountTask("etl.LoadProcess", "WasSuccessful=1") { DisableLogging = true }.Count().Rows);
        }

        [Fact]
        public void ReadLastAbortedProcess()
        {
            //Arrange
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
            Assert.Equal(3, new RowCountTask("etl.LoadProcess", "IsFinished=1") { DisableLogging = true }.Count().Rows);
            Assert.Equal(2, new RowCountTask("etl.LoadProcess", "WasSuccessful=1") { DisableLogging = true }.Count().Rows);
            Assert.Equal(1, new RowCountTask("etl.LoadProcess", "WasAborted=1") { DisableLogging = true }.Count().Rows);
        }

        [Fact]
        public void IsLoadProcessKeySetIfRestarted()
        {
            //Arrange
            StartLoadProcessTask.Start("Test process 13");
            int? processKey1 = ControlFlow.CurrentLoadProcess.LoadProcessKey;
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.Equal(2, new RowCountTask("etl.Log", $"Message='Test Task' AND LoadProcessKey = {processKey1}")
            { DisableLogging = true }.Count().Rows);

            //Act
            StartLoadProcessTask.Start("Test process 14");
            int? processKey2 = ControlFlow.CurrentLoadProcess.LoadProcessKey;

            //Assert
            Assert.NotEqual(processKey1, processKey2);
            SqlTask.ExecuteNonQuery("Test Task", "Select 1 as test");
            Assert.Equal(2, new RowCountTask("etl.Log", $"Message='Test Task' AND LoadProcessKey = {processKey2}")
            { DisableLogging = true }.Count().Rows);
        }

        [Fact]
        public void IsLoadProcessKeySetForLogTask()
        {
            //Arrange
            StartLoadProcessTask.Start("Test process 15");
            int? processKey1 = ControlFlow.CurrentLoadProcess.LoadProcessKey;
            //Act
            LogTask.Error("Test1");
            LogTask.Warn("Test2");
            LogTask.Info("Test3");
            //Assert
            Assert.Equal(3, new RowCountTask("etl.Log",
                $"Message like 'Test%' AND LoadProcessKey = {processKey1}")
                { DisableLogging = true }.Count().Rows);
        }



    }
}
