using ALE.ETLBox.ConnectionManager;
using TestNonParallel.Fixtures;
using TestShared.Helper;

namespace TestNonParallel
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

        public static IEnumerable<object[]> AllSqlConnectionsWithoutClickHouse =>
            Config.AllSqlConnectionsWithoutClickHouse("Logging");
    }
}
