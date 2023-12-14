using System.Dynamic;
using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestDatabaseConnectors.src.Fixtures;
using TestShared.src.SharedFixtures;

namespace TestDatabaseConnectors.src.DBSource
{
    public class DbSourceDynamicObjectTests : DatabaseConnectorsTestBase
    {
        public DbSourceDynamicObjectTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        [Theory, MemberData(nameof(Connections))]
        public void SourceAndDestinationSameColumns(IConnectionManager connection)
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(
                connection,
                "SourceDynamic"
            );
            source2Columns.InsertTestData();
            var dest2Columns = new TwoColumnsTableFixture(
                connection,
                "DestinationDynamic"
            );

            //Act
            var source = new DbSource<ExpandoObject>(
                connection,
                "SourceDynamic"
            );
            var dest = new DbDestination<ExpandoObject>(
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
