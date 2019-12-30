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
    public class GetLoadProcessAsJSONTaskTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("Logging");
        public GetLoadProcessAsJSONTaskTests(LoggingDatabaseFixture dbFixture)
        {
            CreateLogTablesTask.CreateLog(Connection, "Log");
        }

        public void Dispose()
        {
            RemoveLogTablesTask.Remove(Connection);
        }

        private void RunProcess1()
        {
            StartLoadProcessTask.Start(Connection, "Process 1", "Start");
            SqlTask.ExecuteNonQuery(Connection, $"Just some sql", "Select 1 as test");
            EndLoadProcessTask.End(Connection, "End");
        }

        [Fact]
        public void Get1LoadProcessAsJSON()
        {
            //Arrange
            RunProcess1();

            //Act
            string response = GetLoadProcessAsJSONTask.GetJSON(Connection);
            JArray json = JArray.Parse(response);

            //Assert
            Assert.Equal("Process 1", (string)json[0]["processName"]);
            Assert.False((bool)json[0]["isRunning"]);
            Assert.True((bool)json[0]["wasSuccessful"]);
            Assert.False((bool)json[0]["wasAborted"]);
            Assert.True((bool)json[0]["isFinished"]);
            Assert.False((bool)json[0]["isTransferCompleted"]);
            Assert.Equal("Start", (string)json[0]["startMessage"]);
            Assert.Equal("End", (string)json[0]["endMessage"]);
        }
    }
}
