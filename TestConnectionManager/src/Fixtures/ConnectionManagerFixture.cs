using ETLBoxTests.Helper;
using Xunit;

namespace ETLBoxTests.Fixtures
{
    [CollectionDefinition("Connection Manager")]
    public class CollectionConnectionManagerFixture : ICollectionFixture<ConnectionManagerFixture> { }
    public class ConnectionManagerFixture
    {
        public ConnectionManagerFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("ConnectionManager");
        }
    }

}
