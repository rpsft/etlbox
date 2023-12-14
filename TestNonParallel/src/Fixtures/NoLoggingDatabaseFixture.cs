using ALE.ETLBox.src.Toolbox.ConnectionManager.Native;
using TestShared.src.Helper;

namespace ALE.ETLBoxTests.NonParallel.src.Fixtures
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
