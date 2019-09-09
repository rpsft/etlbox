using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.BigData
{
    [CollectionDefinition("Big Data")]
    public class BigDataCollectionClass : ICollectionFixture<BigDataDatabaseFixture> { }

    public class BigDataDatabaseFixture
    {
        public BigDataDatabaseFixture()
        {
            DatabaseHelper.RecreateDatabase(Config.SqlConnectionString("BigData").DBName,
                Config.SqlConnectionString("BigData"));
        }
    }
}
