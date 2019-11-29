using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.Performance
{
    [CollectionDefinition("Performance")]
    public class BigDataCollectionClass : ICollectionFixture<BigDataDatabaseFixture> { }

    public class BigDataDatabaseFixture
    {
        public BigDataDatabaseFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("BigData");
        }
    }
}
