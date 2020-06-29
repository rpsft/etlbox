using ETLBoxTests.Helper;
using Xunit;

namespace ETLBoxTests.Logging
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
            DatabaseHelper.CleanUpOracle("Logging");
            DatabaseHelper.RecreateMariaDbDatabase("Logging");
        }
    }
}
