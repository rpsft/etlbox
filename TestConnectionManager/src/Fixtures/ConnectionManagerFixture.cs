using ALE.ETLBox.Helper;
using Xunit;

namespace ALE.ETLBoxTests.Fixtures
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
