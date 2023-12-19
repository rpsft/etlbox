using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestDatabaseConnectors.Fixtures;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBSource
{
    public class DbSourceTests : DatabaseConnectorsTestBase
    {
        public DbSourceTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        public class MySimpleRow
        {
            public long Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void SimpleFlow(IConnectionManager connection)
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(
                connection,
                "DbSourceSimple"
            );
            source2Columns.InsertTestData();
            var dest2Columns = new TwoColumnsTableFixture(
                connection,
                "DbDestinationSimple"
            );

            //Act
            var source = new DbSource<MySimpleRow>(connection, "DbSourceSimple");
            var dest = new DbDestination<MySimpleRow>(
                connection,
                "DbDestinationSimple"
            );

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
