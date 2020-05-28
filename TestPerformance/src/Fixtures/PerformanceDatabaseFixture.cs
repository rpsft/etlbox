using ETLBox.Helper;
using Xunit;

namespace ETLBoxTests.Performance
{
    [CollectionDefinition("Performance")]
    public class PerformanceCollectionClass : ICollectionFixture<PerformanceDatabaseFixture> { }

    public class PerformanceDatabaseFixture
    {
        public PerformanceDatabaseFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("Performance");
            DatabaseHelper.RecreateMySqlDatabase("Performance");
            DatabaseHelper.RecreatePostgresDatabase("Performance");
        }
    }
}
