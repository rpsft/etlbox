using ALE.ETLBox.ConnectionManager;
using TestDatabaseConnectors.Fixtures;
using TestShared.Helper;

namespace TestDatabaseConnectors
{
    [CollectionDefinition("DatabaseConnectors", DisableParallelization = false)]
    public class DataFlowSourceDestinationCollectionClass
        : ICollectionFixture<DatabaseSourceDestinationFixture>
    { }

    [Collection("DatabaseConnectors")]
    public class DatabaseConnectorsTestBase
    {
        private const string SourceConfigSection =
            DatabaseSourceDestinationFixture.SourceConfigSection;

        private const string DestinationConfigSection =
            DatabaseSourceDestinationFixture.DestinationConfigSection;

        private const string OtherConfigSection =
            DatabaseSourceDestinationFixture.OtherConfigSection;

        protected readonly DatabaseSourceDestinationFixture Fixture;

        protected DatabaseConnectorsTestBase(DatabaseSourceDestinationFixture fixture)
        {
            Fixture = fixture;
        }

        protected static SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager(SourceConfigSection);

        protected AccessOdbcConnectionManager AccessOdbcConnection =>
            Config.AccessOdbcConnection.ConnectionManager(OtherConfigSection);

        protected static SqlConnectionManager AzureSqlConnection =>
            Config.AzureSqlConnection.ConnectionManager(OtherConfigSection);

        public static IEnumerable<object[]> AllSqlConnections =>
            Config.AllSqlConnections(SourceConfigSection);

        protected static IEnumerable<CultureInfo> AllLocalCultures => Config.AllLocalCultures();

        public static IEnumerable<object[]> AllConnectionsWithoutSQLite =>
            Config.AllConnectionsWithoutSQLite(SourceConfigSection);

        public static IEnumerable<object[]> AllOdbcConnections =>
            Config.AllOdbcConnections(OtherConfigSection);

        public static IEnumerable<object[]> MixedSourceDestinations() =>
            new[]
            {
                //Same DB
                new object[]
                {
                    Config.SqlConnection.ConnectionManager(SourceConfigSection),
                    Config.SqlConnection.ConnectionManager(DestinationConfigSection)
                },
                new object[]
                {
                    Config.SQLiteConnection.ConnectionManager(SourceConfigSection),
                    Config.SQLiteConnection.ConnectionManager(DestinationConfigSection)
                },
                new object[]
                {
                    Config.MySqlConnection.ConnectionManager(SourceConfigSection),
                    Config.MySqlConnection.ConnectionManager(DestinationConfigSection)
                },
                new object[]
                {
                    Config.PostgresConnection.ConnectionManager(SourceConfigSection),
                    Config.PostgresConnection.ConnectionManager(DestinationConfigSection)
                },
                //Mixed
                new object[]
                {
                    Config.SqlConnection.ConnectionManager(SourceConfigSection),
                    Config.SQLiteConnection.ConnectionManager(DestinationConfigSection)
                },
                new object[]
                {
                    Config.SQLiteConnection.ConnectionManager(SourceConfigSection),
                    Config.SqlConnection.ConnectionManager(DestinationConfigSection)
                },
                new object[]
                {
                    Config.MySqlConnection.ConnectionManager(SourceConfigSection),
                    Config.PostgresConnection.ConnectionManager(DestinationConfigSection)
                },
                new object[]
                {
                    Config.SqlConnection.ConnectionManager(SourceConfigSection),
                    Config.PostgresConnection.ConnectionManager(DestinationConfigSection)
                }
            };
    }
}
