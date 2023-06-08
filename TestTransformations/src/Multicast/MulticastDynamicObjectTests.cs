using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestTransformations.Multicast
{
    [Collection("DataFlow")]
    public class MulticastDynamicObjectTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SplitInto2Tables()
        {
            //Arrange
            TwoColumnsTableFixture sourceTable = new TwoColumnsTableFixture("Source");
            sourceTable.InsertTestData();
            TwoColumnsTableFixture dest1Table = new TwoColumnsTableFixture("Destination1");
            TwoColumnsTableFixture dest2Table = new TwoColumnsTableFixture("Destination2");

            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>(Connection, "Source");
            DbDestination<ExpandoObject> dest1 = new DbDestination<ExpandoObject>(
                Connection,
                "Destination1"
            );
            DbDestination<ExpandoObject> dest2 = new DbDestination<ExpandoObject>(
                Connection,
                "Destination2"
            );

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
