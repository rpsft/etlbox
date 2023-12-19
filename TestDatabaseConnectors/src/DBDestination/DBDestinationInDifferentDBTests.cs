using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestDatabaseConnectors.Fixtures;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBDestination
{
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
            var source2Columns = new TwoColumnsTableFixture(
                sourceConnection,
                "Source"
            );
            source2Columns.InsertTestData();
            var dest2Columns = new TwoColumnsTableFixture(
                destConnection,
                "Destination"
            );

            //Act
            var source = new DbSource<string[]>(sourceConnection, "Source");
            var dest = new DbDestination<string[]>(
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
