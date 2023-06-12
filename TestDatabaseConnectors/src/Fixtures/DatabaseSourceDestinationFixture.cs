using TestShared.Helper;

namespace TestDatabaseConnectors.Fixtures
{
    public sealed class DatabaseSourceDestinationFixture : IDisposable
    {
        public const string SourceConfigSection = "DataFlowSource";
        public const string DestinationConfigSection = "DataFlowDestination";

        public DatabaseSourceDestinationFixture()
        {
            DatabaseHelper.RecreateDatabase(Config.SqlConnection, SourceConfigSection);
            DatabaseHelper.RecreateDatabase(Config.SqlConnection, DestinationConfigSection);
            DatabaseHelper.RecreateDatabase(Config.MySqlConnection, SourceConfigSection);
            DatabaseHelper.RecreateDatabase(Config.MySqlConnection, DestinationConfigSection);
            DatabaseHelper.RecreateDatabase(Config.PostgresConnection, SourceConfigSection);
            DatabaseHelper.RecreateDatabase(Config.PostgresConnection, DestinationConfigSection);
        }

        public void Dispose()
        {
            DatabaseHelper.RecreateDatabase(Config.SqlConnection, SourceConfigSection);
            DatabaseHelper.RecreateDatabase(Config.SqlConnection, DestinationConfigSection);
            DatabaseHelper.RecreateDatabase(Config.MySqlConnection, SourceConfigSection);
            DatabaseHelper.RecreateDatabase(Config.MySqlConnection, DestinationConfigSection);
            DatabaseHelper.RecreateDatabase(Config.PostgresConnection, SourceConfigSection);
            DatabaseHelper.RecreateDatabase(Config.PostgresConnection, DestinationConfigSection);
        }
    }
}
