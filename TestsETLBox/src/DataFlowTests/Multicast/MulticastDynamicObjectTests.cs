using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class MulticastDynamicObjectTests
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public MulticastDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SplitInto2Tables()
        {
            //Arrange
            TwoColumnsTableFixture sourceTable = new TwoColumnsTableFixture("Source");
            sourceTable.InsertTestData();
            TwoColumnsTableFixture dest1Table = new TwoColumnsTableFixture("Destination1");
            TwoColumnsTableFixture dest2Table = new TwoColumnsTableFixture("Destination2");

            DBSource<ExpandoObject> source = new DBSource<ExpandoObject>(Connection, "Source");
            DBDestination<ExpandoObject> dest1 = new DBDestination<ExpandoObject>(Connection, "Destination1");
            DBDestination< ExpandoObject> dest2 = new DBDestination<ExpandoObject>(Connection, "Destination2");

            //Act
            Multicast<ExpandoObject> multicast = new Multicast<ExpandoObject>();

            source.LinkTo(multicast);
            multicast.LinkTo(dest1);
            multicast.LinkTo(dest2);
            source.Execute();
            dest1.Wait();
            dest2.Wait();

            //Assert
            dest1Table.AssertTestData();
            dest2Table.AssertTestData();
        }

    }
}
