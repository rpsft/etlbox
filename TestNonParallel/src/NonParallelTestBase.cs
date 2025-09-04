using ALE.ETLBox.ConnectionManager;
using ALE.ETLBoxTests.NonParallel.Fixtures;
using ETLBox.Primitives;
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

        public static TheoryData<IConnectionManager> AllSqlConnections =>
            new(Config.AllSqlConnections("Logging"));

        public static TheoryData<IConnectionManager> AllSqlConnectionsWithoutClickHouse =>
            new(Config.AllConnectionsWithoutClickHouse("Logging"));
    }
}
