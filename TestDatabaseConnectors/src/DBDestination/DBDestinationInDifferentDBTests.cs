using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBDestination
{
    [Collection("DataFlow Source and Destination")]
    public class DbDestinationDifferentDBTests
    {
        public static IEnumerable<object[]> MixedSourceDestinations() =>
            new[]
            {
                //Same DB
                new object[]
                {
                    Config.SqlConnection.ConnectionManager("DataFlowSource"),
                    Config.SqlConnection.ConnectionManager("DataFlowDestination")
                },
                new object[]
                {
                    Config.SQLiteConnection.ConnectionManager("DataFlowSource"),
                    Config.SQLiteConnection.ConnectionManager("DataFlowDestination")
                },
                new object[]
                {
                    Config.MySqlConnection.ConnectionManager("DataFlowSource"),
                    Config.MySqlConnection.ConnectionManager("DataFlowDestination")
                },
                new object[]
                {
                    Config.PostgresConnection.ConnectionManager("DataFlowSource"),
                    Config.PostgresConnection.ConnectionManager("DataFlowDestination")
                },
                //Mixed
                new object[]
                {
                    Config.SqlConnection.ConnectionManager("DataFlowSource"),
                    Config.SQLiteConnection.ConnectionManager("DataFlowDestination")
                },
                new object[]
                {
                    Config.SQLiteConnection.ConnectionManager("DataFlowSource"),
                    Config.SqlConnection.ConnectionManager("DataFlowDestination")
                },
                new object[]
                {
                    Config.MySqlConnection.ConnectionManager("DataFlowSource"),
                    Config.PostgresConnection.ConnectionManager("DataFlowDestination")
                },
                new object[]
                {
                    Config.SqlConnection.ConnectionManager("DataFlowSource"),
                    Config.PostgresConnection.ConnectionManager("DataFlowDestination")
                }
            };

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
