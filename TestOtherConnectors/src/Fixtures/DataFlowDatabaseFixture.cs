using ALE.ETLBox.Helper;
using Xunit;

namespace ALE.ETLBoxTests.Fixtures
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
