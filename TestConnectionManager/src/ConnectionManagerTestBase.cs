using TestConnectionManager.Fixtures;
using TestShared.Helper;

namespace TestConnectionManager
{
    [Collection("Connection Manager")]
    public class ConnectionManagerTestBase
    {
        protected ConnectionManagerTestBase(ConnectionManagerFixture fixture) { }

        protected static string ConnectionStringParameter =>
            Config.SqlConnection.RawConnectionString("ConnectionManager");
    }
}
