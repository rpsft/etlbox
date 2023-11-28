using TestConnectionManager.src.Fixtures;
using TestShared.Helper;

namespace TestConnectionManager.src
{
    [Collection("Connection Manager")]
    public class ConnectionManagerTestBase
    {
        protected ConnectionManagerTestBase(ConnectionManagerFixture fixture) { }

        protected static string ConnectionStringParameter =>
            Config.SqlConnection.RawConnectionString("ConnectionManager");

        protected static string ClickHouseConnectionStringParameter =>
            Config.ClickHouseConnection.RawConnectionString("ConnectionManager");
    }
}
