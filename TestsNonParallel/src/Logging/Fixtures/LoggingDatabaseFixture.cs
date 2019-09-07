using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.Logging
{
    [CollectionDefinition("Logging")]
    public class LoggingCollectionClass : ICollectionFixture<LoggingDatabaseFixture> { }

    public class LoggingDatabaseFixture
    {
        public LoggingDatabaseFixture()
        {
            DatabaseHelper.RecreateDatabase(Config.SqlConnectionString("Logging").DBName, Config.SqlConnectionString("Logging"));
            ControlFlow.SetLoggingDatabase(Config.SqlConnectionManager("Logging"));
        }
    }
}
