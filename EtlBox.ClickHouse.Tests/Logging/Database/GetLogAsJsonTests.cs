using System.Text.RegularExpressions;
using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.TaskBase.ControlFlow;
using ALE.ETLBox.src.Toolbox.ControlFlow;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using ALE.ETLBox.src.Toolbox.Logging;
using EtlBox.Database.Tests.Infrastructure;
using EtlBox.Logging.Database;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.Logging.Database
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class GetLogAsJsonTests : DatabaseTestBase, IDisposable
    {
        protected GetLogAsJsonTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
            CreateLoadProcessTableTask.Create(_connectionManager);
            CreateLogTableTask.Create(_connectionManager);
            DatabaseLoggingConfiguration.AddDatabaseLoggingConfiguration(_connectionManager);
            ALE.ETLBox.src.Toolbox.ControlFlow.ControlFlow.DefaultDbConnection = _connectionManager;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            DropTableTask.Drop(_connectionManager, ALE.ETLBox.src.Toolbox.ControlFlow.ControlFlow.LogTable);
            DropTableTask.Drop(_connectionManager, ALE.ETLBox.src.Toolbox.ControlFlow.ControlFlow.LoadProcessTable);
            ALE.ETLBox.src.Toolbox.ControlFlow.ControlFlow.ClearSettings();
        }

        [Fact]
        public void TestGetDemoLogAsJSON()
        {
            //Arrange
            RunDemoProcess();
            var jsonresult = GetLogAsJSONTask.GetJSON();

            //Act
            jsonresult = NormalizeJsonResult(jsonresult);

            //Assert
            var expectedresult = NormalizeJsonResult(
                File.ReadAllText("res/Demo/demolog_tobe.json")
            );
            Assert.Equal(expectedresult, jsonresult);
        }

        private IConnectionManager _connectionManager
            => _fixture.GetContainer(_connectionType).GetConnectionManager();

        private string NormalizeJsonResult(string jsonresult)
        {
            return RemoveLineEndings(RemoveHashes(RemoveDates(jsonresult.ToLower().Trim())));
        }

        private static string RemoveLineEndings(string originalJson)
        {
            return Regex.Replace(originalJson, "[\n\r]", "");
        }

        private static string RemoveHashes(string jsonresult) =>
            Regex.Replace(jsonresult, @"""taskhash"": ""[A-Za-z0-9]*""", @"""taskHash"": """"");

        private static string RemoveDates(string jsonresult) =>
            Regex.Replace(
                jsonresult,
                @"[0-9]+-[0-9]+-[0-9]([Tt]|\w)+[0-9]+:[0-9]+:[0-9]+(?:\.[0-9]+)?",
                ""
            );

        private void RunDemoProcess()
        {
            new Sequence("Test sequence 1", RunSubSequence) { TaskType = "SUBPACKAGE" }.Execute();
            SqlTask.ExecuteNonQuery("Sql #1", "Select 1 as test");
            LogTask.Info("Info message");
        }

        private void RunSubSequence()
        {
            Sequence.Execute(
                "Test sub sequence 1.1",
                () =>
                {
                    SqlTask.ExecuteNonQuery("Sql #2", "Select 1 as test");
                    SqlTask.ExecuteNonQuery("Sql #3", "Select 1 as test");
                    LogTask.Warn("Warn message #1");
                }
            );
            Sequence.Execute(
                "Test sub sequence 1.2",
                () =>
                {
                    SqlTask.ExecuteNonQuery("Sql #4", "Select 1 as test");
                }
            );
            Sequence.Execute(
                "Test sub sequence 1.3",
                () =>
                {
                    Sequence.Execute(
                        "Test sub sequence 2.1",
                        () =>
                        {
                            Sequence.Execute(
                                "Test sub sequence 3.1",
                                () =>
                                {
                                    SqlTask.ExecuteNonQuery("Sql #5", "Select 1 as test");
                                    SqlTask.ExecuteNonQuery("Sql #6", "Select 1 as test");
                                    LogTask.Warn("Warn message #2");
                                }
                            );
                            CustomTask.Execute("Custom #1", () => { });
                            SqlTask.ExecuteNonQuery("Sql #7", "Select 1 as test");
                        }
                    );
                    Sequence.Execute(
                        "Test sub sequence 2.2",
                        () =>
                        {
                            CustomTask.Execute("Custom #2", () => { });
                            SqlTask.ExecuteNonQuery("Sql #7", "Select 1 as test");
                        }
                    );
                    Sequence.Execute(
                        "Test sub sequence 2.3",
                        () =>
                        {
                            SqlTask.ExecuteNonQuery("Sql #8", "Select 1 as test");
                            CustomTask.Execute("Custom #2", () => { });
                            Sequence.Execute(
                                "Test sub sequence 3.3",
                                () =>
                                {
                                    SqlTask.ExecuteNonQuery("Sql #9", "Select 1 as test");
                                    SqlTask.ExecuteNonQuery("Sql #10", "Select 1 as test");
                                    LogTask.Error("Error message");
                                }
                            );
                        }
                    );
                }
            );
            CustomTask.Execute("Custom #3", () => { });
        }

        public class SqlServer : GetLogAsJsonTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger) 
                : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : GetLogAsJsonTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger)
                : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }

        public class ClickHouse : GetLogAsJsonTests
        {
            public ClickHouse(DatabaseFixture fixture, ITestOutputHelper logger)
                : base(fixture, ConnectionManagerType.ClickHouse, logger)
            {
            }
        }
    }
}
