using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using ALE.ETLBox.src.Toolbox.Logging;
using ALE.ETLBoxTests.NonParallel.src.Fixtures;
using EtlBox.Logging.Database;
using Newtonsoft.Json.Linq;

namespace ALE.ETLBoxTests.NonParallel.src.Logging.LoadProcessTable
{
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
            DropTableTask.Drop(SqlConnection, ETLBox.src.Toolbox.ControlFlow.ControlFlow.LogTable);
            DropTableTask.Drop(SqlConnection, ETLBox.src.Toolbox.ControlFlow.ControlFlow.LoadProcessTable);
            ETLBox.src.Toolbox.ControlFlow.ControlFlow.ClearSettings();
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
            var response = GetLoadProcessAsJSONTask.GetJSON(SqlConnection);
            var json = JArray.Parse(response);

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
