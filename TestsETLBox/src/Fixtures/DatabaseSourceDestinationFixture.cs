using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.Fixtures
{
    [CollectionDefinition("DataFlow Source and Destination")]
    public class DatalFlowSourceDestinationCollectionClass : ICollectionFixture<DatabaseSourceDestinationFixture> { }
    public class DatabaseSourceDestinationFixture
    {
        public DatabaseSourceDestinationFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("DataFlowSource");
            DatabaseHelper.RecreateSqlDatabase("DataFlowDestination");
        }
    }

}
