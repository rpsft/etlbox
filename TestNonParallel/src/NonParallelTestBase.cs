using ALE.ETLBox.src.Toolbox.ConnectionManager.Native;
using ALE.ETLBoxTests.NonParallel.src.Fixtures;
using TestShared.src.Helper;

namespace ALE.ETLBoxTests.NonParallel.src
{
    [CollectionDefinition("Logging")]
    public class LoggingCollectionClass : ICollectionFixture<LoggingDatabaseFixture> { }

    [Collection("Logging")]
    public class NonParallelTestBase
    {
        protected LoggingDatabaseFixture Fixture;

        public NonParallelTestBase(LoggingDatabaseFixture fixture)
        {
            Fixture = fixture;
        }

        protected static SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("Logging");

        public static IEnumerable<object[]> AllSqlConnections =>
            Config.AllSqlConnections("Logging");
    }
}
