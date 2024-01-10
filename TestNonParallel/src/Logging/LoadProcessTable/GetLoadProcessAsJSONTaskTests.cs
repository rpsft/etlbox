using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Logging;
using ETLBox.Logging.Database;
using Newtonsoft.Json.Linq;
using TestNonParallel.Fixtures;

namespace TestNonParallel.Logging.LoadProcessTable
{
    [Collection("Logging")]
    public sealed class GetLoadProcessAsJSONTaskTests : NonParallelTestBase, IDisposable
    {
        public GetLoadProcessAsJSONTaskTests(LoggingDatabaseFixture fixture)
            : base(fixture)
        {
            CreateLogTableTask.Create(SqlConnection);
            CreateLoadProcessTableTask.Create(SqlConnection);
            DatabaseLoggingConfiguration.AddDatabaseLoggingConfiguration(SqlConnection);
        }

        public void Dispose()
        {
            DropTableTask.Drop(SqlConnection, ALE.ETLBox.Common.ControlFlow.ControlFlow.LogTable);
            DropTableTask.Drop(
                SqlConnection,
                ALE.ETLBox.Common.ControlFlow.ControlFlow.LoadProcessTable
            );
            ALE.ETLBox.Common.ControlFlow.ControlFlow.ClearSettings();
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
