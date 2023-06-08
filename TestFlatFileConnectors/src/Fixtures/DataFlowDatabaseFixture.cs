using TestShared.Helper;
using Xunit;

namespace TestFlatFileConnectors.Fixtures
{
    [CollectionDefinition("DataFlow")]
    public class DatalFlowCollectionClass : ICollectionFixture<DataFlowDatabaseFixture> { }

    public class DataFlowDatabaseFixture
    {
        public DataFlowDatabaseFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("DataFlow");
            DatabaseHelper.RecreateMySqlDatabase("DataFlow");
            DatabaseHelper.RecreatePostgresDatabase("DataFlow");
        }
    }
}
