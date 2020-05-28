using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.Helper;
using ETLBox.Logging;
using Newtonsoft.Json.Linq;
using System;
using Xunit;

namespace ETLBoxTests.Logging
{
    [Collection("Logging")]
    public class GetLoadProcessAsJSONTaskTests : IDisposable
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("Logging");
        public GetLoadProcessAsJSONTaskTests(LoggingDatabaseFixture dbFixture)
        {
            CreateLogTableTask.Create(SqlConnection);
            CreateLoadProcessTableTask.Create(SqlConnection);
            ControlFlow.AddLoggingDatabaseToConfig(SqlConnection);
        }

        public void Dispose()
        {
            DropTableTask.Drop(SqlConnection, ControlFlow.LogTable);
            DropTableTask.Drop(SqlConnection, ControlFlow.LoadProcessTable);
            ControlFlow.ClearSettings();
        }

        private void RunProcess1()
        {
            StartLoadProcessTask.Start(SqlConnection, "Process 1", "Start");
            SqlTask.ExecuteNonQuery(SqlConnection, $"Just some sql", "Select 1 as test");
            EndLoadProcessTask.End(SqlConnection, "End");
        }

        [Fact]
        public void Get1LoadProcessAsJSON()
        {
            //Arrange
            RunProcess1();

            //Act
            string response = GetLoadProcessAsJSONTask.GetJSON(SqlConnection);
            JArray json = JArray.Parse(response);

            //Assert
            Assert.Equal("Process 1", (string)json[0]["processName"]);
            Assert.False((bool)json[0]["isRunning"]);
            Assert.True((bool)json[0]["wasSuccessful"]);
            Assert.False((bool)json[0]["wasAborted"]);
            Assert.True((bool)json[0]["isFinished"]);
            Assert.Equal("Start", (string)json[0]["startMessage"]);
            Assert.Equal("End", (string)json[0]["endMessage"]);
        }
    }
}
