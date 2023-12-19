using ALE.ETLBox.ConnectionManager;
using ALE.ETLBoxTests.NonParallel.Fixtures;
using TestShared.Helper;

namespace ALE.ETLBoxTests.NonParallel
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
