using ALE.ETLBox.ConnectionManager;
using JetBrains.Annotations;
using TestShared.Helper;

namespace ALE.ETLBoxTests.NonParallel.Fixtures
{
    [UsedImplicitly]
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
