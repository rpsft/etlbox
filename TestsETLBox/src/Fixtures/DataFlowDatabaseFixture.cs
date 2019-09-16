using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.Fixtures
{
    [CollectionDefinition("DataFlow")]
    public class DatalFlowCollectionClass : ICollectionFixture<DataFlowDatabaseFixture> { }
    public class DataFlowDatabaseFixture
    {
        public DataFlowDatabaseFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("DataFlow");
            DatabaseHelper.RecreateMySqlDatabase("DataFlow");
        }


    }

}
