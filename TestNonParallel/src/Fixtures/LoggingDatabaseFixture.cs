using JetBrains.Annotations;
using TestShared.src.Helper;

namespace ALE.ETLBoxTests.NonParallel.src.Fixtures
{
    [UsedImplicitly]
    public sealed class LoggingDatabaseFixture : IDisposable
    {
        public LoggingDatabaseFixture()
        {
            DatabaseHelper.RecreateDatabase(Config.SqlConnection, "Logging");
            DatabaseHelper.RecreateDatabase(Config.MySqlConnection, "Logging");
            DatabaseHelper.RecreateDatabase(Config.PostgresConnection, "Logging");
        }

        public void Dispose()
        {
            DatabaseHelper.DropDatabase(Config.PostgresConnection, "Logging");
            DatabaseHelper.DropDatabase(Config.MySqlConnection, "Logging");
            DatabaseHelper.DropDatabase(Config.SqlConnection, "Logging");
        }
    }
}
