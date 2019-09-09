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
using System.IO;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class MulticastTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public MulticastTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void DuplicateDataInto3Destinations()
        {
            //Arrange
            TwoColumnsTableFixture sourceTable = new TwoColumnsTableFixture("Source");
            sourceTable.InsertTestData();
            TwoColumnsTableFixture dest1Table = new TwoColumnsTableFixture("Destination1");
            TwoColumnsTableFixture dest2Table = new TwoColumnsTableFixture("Destination2");
            TwoColumnsTableFixture dest3Table = new TwoColumnsTableFixture("Destination3");

            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(Connection, "Source");
            DBDestination<MySimpleRow> dest1 = new DBDestination<MySimpleRow>(Connection, "Destination1");
            DBDestination<MySimpleRow> dest2 = new DBDestination<MySimpleRow>(Connection, "Destination2");
            DBDestination<MySimpleRow> dest3 = new DBDestination<MySimpleRow>(Connection, "Destination3");

            //Act
            Multicast<MySimpleRow> multicast = new Multicast<MySimpleRow>();
            source.LinkTo(multicast);
            multicast.LinkTo(dest1);
            multicast.LinkTo(dest2);
            multicast.LinkTo(dest3);
            source.Execute();
            dest1.Wait();
            dest2.Wait();
            dest3.Wait();

            //Assert
            dest1Table.AssertTestData();
            dest2Table.AssertTestData();
            dest3Table.AssertTestData();
        }

    }
}
