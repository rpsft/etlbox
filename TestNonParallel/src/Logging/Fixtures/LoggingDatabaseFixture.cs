using ALE.ETLBox.Helper;
using Xunit;

namespace ALE.ETLBoxTests.Logging
{
    [CollectionDefinition("Logging")]
    public class LoggingCollectionClass : ICollectionFixture<LoggingDatabaseFixture> { }

    public class LoggingDatabaseFixture
    {
        public LoggingDatabaseFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("Logging");
            DatabaseHelper.RecreateMySqlDatabase("Logging");
            DatabaseHelper.RecreatePostgresDatabase("Logging");
        }
    }
}
