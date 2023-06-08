using System.Diagnostics.CodeAnalysis;
using TestShared.Helper;
using Xunit;

namespace TestConnectionManager.Fixtures
{
    [CollectionDefinition("Connection Manager", DisableParallelization = true)]
    public class CollectionConnectionManagerFixture
        : ICollectionFixture<ConnectionManagerFixture> { }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global")]
    public class ConnectionManagerFixture
    {
        public ConnectionManagerFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("ConnectionManager");
        }
    }
}
