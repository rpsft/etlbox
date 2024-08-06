using TestConnectionManager.Fixtures;
using TestShared.Helper;

namespace TestConnectionManager
{
    [Collection("Connection Manager")]
    public class ConnectionManagerTestBase
    {
        protected ConnectionManagerTestBase(ConnectionManagerFixture fixture) { }

        protected static string SqlConnectionStringParameter =>
            Config.SqlConnection.RawConnectionString("ConnectionManager");

        protected static string PostgresConnectionStringParameter =>
            Config.PostgresConnection.RawConnectionString("ConnectionManager");
    }
}
