using ETLBox.ConnectionManager;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class MulticastStringArrayTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public MulticastStringArrayTests(DataFlowDatabaseFixture dbFixture)
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

            DbSource<string[]> source = new DbSource<string[]>(Connection, "Source");
            DbDestination<string[]> dest1 = new DbDestination<string[]>(Connection, "Destination1");
            DbDestination<string[]> dest2 = new DbDestination<string[]>(Connection, "Destination2");

            //Act
            Multicast<string[]> multicast = new Multicast<string[]>();

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
