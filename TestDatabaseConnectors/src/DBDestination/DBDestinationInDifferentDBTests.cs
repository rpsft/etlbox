using ALE.ETLBox.DataFlow;
using ETLBox.Primitives;

namespace TestDatabaseConnectors.DBDestination
{
    [Collection("DatabaseConnectors")]
    public class DbDestinationDifferentDBTests : DatabaseConnectorsTestBase
    {
        public DbDestinationDifferentDBTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        [Theory, MemberData(nameof(MixedSourceDestinations))]
        public void TestTransferBetweenDBs(
            IConnectionManager sourceConnection,
            IConnectionManager destConnection
        )
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                sourceConnection,
                "Source"
            );
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                destConnection,
                "Destination"
            );

            //Act
            DbSource<string[]> source = new DbSource<string[]>(sourceConnection, "Source");
            DbDestination<string[]> dest = new DbDestination<string[]>(
                destConnection,
                "Destination"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
