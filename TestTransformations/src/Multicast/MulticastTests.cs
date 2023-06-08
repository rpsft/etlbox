using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestTransformations.Multicast
{
    [Collection("DataFlow")]
    public class MulticastTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
            public int Col3 => Col1;
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

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(Connection, "Source");
            DbDestination<MySimpleRow> dest1 = new DbDestination<MySimpleRow>(
                Connection,
                "Destination1"
            );
            DbDestination<MySimpleRow> dest2 = new DbDestination<MySimpleRow>(
                Connection,
                "Destination2"
            );
            DbDestination<MySimpleRow> dest3 = new DbDestination<MySimpleRow>(
                Connection,
                "Destination3"
            );

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
