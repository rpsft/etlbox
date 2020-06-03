using ETLBoxTests.Helper;
using Xunit;

namespace ETLBoxTests.Fixtures
{
    [CollectionDefinition("ControlFlow")]
    public class ControlFlowCollectionClass : ICollectionFixture<ControlFlowDatabaseFixture> { }
    public class ControlFlowDatabaseFixture
    {
        public ControlFlowDatabaseFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("ControlFlow");
            DatabaseHelper.RecreateMySqlDatabase("ControlFlow");
            DatabaseHelper.RecreatePostgresDatabase("ControlFlow");
        }
    }

}
