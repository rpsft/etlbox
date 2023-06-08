using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Logging;
using Newtonsoft.Json.Linq;
using TestShared.Helper;

namespace ALE.ETLBoxTests.NonParallel.Logging.LoadProcessTable
{
    [Collection("Logging")]
    public class GetLoadProcessAsJSONTaskTests : IDisposable
    {
        private SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("Logging");

        public GetLoadProcessAsJSONTaskTests()
        {
            CreateLogTableTask.Create(SqlConnection);
            CreateLoadProcessTableTask.Create(SqlConnection);
            ETLBox.ControlFlow.ControlFlow.AddLoggingDatabaseToConfig(SqlConnection);
        }

        public void Dispose()
        {
            DropTableTask.Drop(SqlConnection, ETLBox.ControlFlow.ControlFlow.LogTable);
            DropTableTask.Drop(SqlConnection, ETLBox.ControlFlow.ControlFlow.LoadProcessTable);
            ETLBox.ControlFlow.ControlFlow.ClearSettings();
        }

        private void RunProcess1()
        {
            StartLoadProcessTask.Start(SqlConnection, "Process 1", "Start");
            SqlTask.ExecuteNonQuery(SqlConnection, "Just some sql", "Select 1 as test");
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
