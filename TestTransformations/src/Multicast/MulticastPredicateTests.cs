using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestTransformations.Multicast
{
    [Collection("DataFlow")]
    public class MulticastPredicateTests
    {
        private SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
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
            var _ = new TwoColumnsTableFixture("Destination1");
            var __ = new TwoColumnsTableFixture("Destination2");

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(Connection, "Source");
            DbDestination<MySimpleRow> dest1 = new DbDestination<MySimpleRow>(
                Connection,
                "Destination1"
            );
            DbDestination<MySimpleRow> dest2 = new DbDestination<MySimpleRow>(
                Connection,
                "Destination2"
            );

            //Act
            Multicast<MySimpleRow> multicast = new Multicast<MySimpleRow>();
            source.LinkTo(multicast);
            multicast.LinkTo(dest1, row => row.Col1 <= 2);
            multicast.LinkTo(dest2, row => row.Col1 > 2);
            source.Execute();
            dest1.Wait();
            dest2.Wait();

            //Assert
            Assert.Equal(
                1,
                RowCountTask.Count(Connection, "Destination1", "Col1 = 1 AND Col2='Test1'")
            );
            Assert.Equal(
                1,
                RowCountTask.Count(Connection, "Destination1", "Col1 = 2 AND Col2='Test2'")
            );
            Assert.Equal(
                1,
                RowCountTask.Count(Connection, "Destination2", "Col1 = 3 AND Col2='Test3'")
            );
        }
    }
}
