using System.Dynamic;
using ALE.ETLBox.DataFlow;
using ETLBox.Primitives;
using TestDatabaseConnectors.Fixtures;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBSource
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
