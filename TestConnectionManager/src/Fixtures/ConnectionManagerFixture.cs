using System.Diagnostics.CodeAnalysis;
using TestShared.Helper;

namespace TestConnectionManager.src.Fixtures
{
    [CollectionDefinition("Connection Manager", DisableParallelization = true)]
    public class CollectionConnectionManagerFixture
        : ICollectionFixture<ConnectionManagerFixture>
    { }

    [SuppressMessage("ReSharper", "ClassNeverInstantiated.Global")]
    public sealed class ConnectionManagerFixture : IDisposable
    {
        public const string Section = "ConnectionManager";

        public ConnectionManagerFixture()
        {
            DatabaseHelper.RecreateDatabase(Config.SqlConnection, Section);
        }

        public void Dispose()
        {
            DatabaseHelper.DropDatabase(Config.SqlConnection, Section);
        }
    }
}
