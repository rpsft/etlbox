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
