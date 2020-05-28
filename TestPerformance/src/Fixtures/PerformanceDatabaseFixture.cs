using ALE.ETLBox.Helper;
using Xunit;

namespace ALE.ETLBoxTests.Performance
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
