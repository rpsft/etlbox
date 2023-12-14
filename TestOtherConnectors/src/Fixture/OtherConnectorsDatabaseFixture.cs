using TestShared.src.Helper;

namespace TestOtherConnectors.src.Fixture
{
    public sealed class OtherConnectorsDatabaseFixture : IDisposable
    {
        private const string Section = "DataFlow";

        private readonly string _dbNameSuffix = null;

        public OtherConnectorsDatabaseFixture()
        {
            DatabaseHelper.RecreateDatabase(Config.SqlConnection, Section, _dbNameSuffix);
        }

        public void Dispose()
        {
            DatabaseHelper.DropDatabase(Config.SqlConnection, Section, _dbNameSuffix);
        }
    }
}
