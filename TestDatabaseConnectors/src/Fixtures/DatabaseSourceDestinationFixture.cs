using ETLBoxTests.Helper;
using Xunit;

namespace ETLBoxTests.Fixtures
{
    [CollectionDefinition("DataFlow Source and Destination")]
    public class DatalFlowSourceDestinationCollectionClass : ICollectionFixture<DatabaseSourceDestinationFixture> { }
    public class DatabaseSourceDestinationFixture
    {
        public DatabaseSourceDestinationFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("DataFlowSource");
            DatabaseHelper.RecreateSqlDatabase("DataFlowDestination");
            DatabaseHelper.RecreateMySqlDatabase("DataFlowSource");
            DatabaseHelper.RecreateMySqlDatabase("DataFlowDestination");
            DatabaseHelper.RecreatePostgresDatabase("DataFlowSource");
            DatabaseHelper.RecreatePostgresDatabase("DataFlowDestination");
        }
    }

}
