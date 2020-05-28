using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class MulticastPredicateTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public MulticastPredicateTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void PredicateFilteringWithInteger()
        {
            //Arrange
            TwoColumnsTableFixture sourceTable = new TwoColumnsTableFixture("Source");
            sourceTable.InsertTestData();
            TwoColumnsTableFixture dest1Table = new TwoColumnsTableFixture("Destination1");
            TwoColumnsTableFixture dest2Table = new TwoColumnsTableFixture("Destination2");

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(Connection, "Source");
            DbDestination<MySimpleRow> dest1 = new DbDestination<MySimpleRow>(Connection, "Destination1");
            DbDestination<MySimpleRow> dest2 = new DbDestination<MySimpleRow>(Connection, "Destination2");

            //Act
            Multicast<MySimpleRow> multicast = new Multicast<MySimpleRow>();
            source.LinkTo(multicast);
            multicast.LinkTo(dest1, row => row.Col1 <= 2);
            multicast.LinkTo(dest2, row => row.Col1 > 2);
            source.Execute();
            dest1.Wait();
            dest2.Wait();

            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "Destination1", "Col1 = 1 AND Col2='Test1'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "Destination1", "Col1 = 2 AND Col2='Test2'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "Destination2", "Col1 = 3 AND Col2='Test3'"));

        }
    }
}
