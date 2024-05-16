using ALE.ETLBox.ConnectionManager;
using ETLBox.Primitives;
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

        public static TheoryData<IConnectionManager> AllSqlConnections =>
            new(Config.AllSqlConnections("Logging"));

        public static TheoryData<IConnectionManager> AllSqlConnectionsWithoutClickHouse =>
            new(Config.AllSqlConnectionsWithoutClickHouse("Logging"));
    }
}
