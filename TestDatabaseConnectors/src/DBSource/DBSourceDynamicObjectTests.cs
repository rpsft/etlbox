using System.Dynamic;
using System.Threading;
using ALE.ETLBox.DataFlow;
using ETLBox.Primitives;

namespace TestDatabaseConnectors.DBSource
{
    [Collection("DatabaseConnectors")]
    public class DbSourceDynamicObjectTests : DatabaseConnectorsTestBase
    {
        public DbSourceDynamicObjectTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

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
            source.Execute(CancellationToken.None);
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
