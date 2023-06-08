using System.Dynamic;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBSource
{
    [Collection("DataFlow")]
    public class DbSourceDynamicObjectTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public static IEnumerable<object[]> ConnectionsNoSQLite =>
            Config.AllConnectionsWithoutSQLite("DataFlow");

        [Theory, MemberData(nameof(Connections))]
        public void SourceAndDestinationSameColumns(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                connection,
                "SourceDynamic"
            );
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                connection,
                "DestinationDynamic"
            );

            //Act
            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>(
                connection,
                "SourceDynamic"
            );
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(
                connection,
                "DestinationDynamic"
            );

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
