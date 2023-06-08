using TestShared.Helper;

namespace ALE.ETLBoxTests.NonParallel.Logging.Fixtures
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
