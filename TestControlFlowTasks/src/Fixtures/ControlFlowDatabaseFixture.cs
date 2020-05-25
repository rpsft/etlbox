using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.Fixtures
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
