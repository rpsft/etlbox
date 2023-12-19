using ALE.ETLBox.ConnectionManager;
using TestShared.Helper;

namespace ALE.ETLBoxTests.NonParallel.Fixtures
{
    public sealed class NoLoggingDatabaseFixture : IDisposable
    {
        public NoLoggingDatabaseFixture()
        {
            DatabaseHelper.RecreateDatabase(Config.SqlConnection, "NoLog");
        }

        public static SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("NoLog");

        public void Dispose()
        {
            DatabaseHelper.DropDatabase(Config.SqlConnection, "NoLog");
        }
    }
}
