using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestDatabaseConnectors.src.Fixtures;
using TestShared.src.SharedFixtures;

namespace TestDatabaseConnectors.src.DBDestination
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
