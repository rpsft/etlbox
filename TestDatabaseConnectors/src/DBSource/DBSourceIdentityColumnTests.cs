using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestDatabaseConnectors.src;
using TestDatabaseConnectors.src.Fixtures;
using TestShared.src.SharedFixtures;

namespace TestDatabaseConnectors.src.DBSource
{
    public class DbSourceIdentityColumnTests : DatabaseConnectorsTestBase
    {
        public DbSourceIdentityColumnTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        public class MyPartialRow
        {
            public string Col2 { get; set; }
            public decimal? Col4 { get; set; }
        }

        private static void DataFlowForIdentityColumn(IConnectionManager connection)
        {
            var source = new DbSource<MyPartialRow>(connection, "Source4Cols");
            var dest = new DbDestination<MyPartialRow>(
                connection,
                "Destination4Cols"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
        }

        [Theory, MemberData(nameof(Connections))]
        private void IdentityColumnsAtTheBeginning(IConnectionManager connection)
        {
            //Arrange
            var source4Columns = new FourColumnsTableFixture(
                connection,
                "Source4Cols",
                identityColumnIndex: 0
            );
            source4Columns.InsertTestData();
            var dest4Columns = new FourColumnsTableFixture(
                connection,
                "Destination4Cols",
                identityColumnIndex: 0
            );

            //Act
            DataFlowForIdentityColumn(connection);

            //Assert
            dest4Columns.AssertTestData();
        }

        [Theory, MemberData(nameof(Connections))]
        private void IdentityColumnInTheMiddle(IConnectionManager connection)
        {
            //Arrange
            var source4Columns = new FourColumnsTableFixture(
                connection,
                "Source4Cols",
                identityColumnIndex: 1
            );
            source4Columns.InsertTestData();
            var dest4Columns = new FourColumnsTableFixture(
                connection,
                "Destination4Cols",
                identityColumnIndex: 2
            );

            //Act
            DataFlowForIdentityColumn(connection);

            //Assert
            dest4Columns.AssertTestData();
        }

        [Theory, MemberData(nameof(Connections))]
        private void IdentityColumnAtTheEnd(IConnectionManager connection)
        {
            //Arrange
            var source4Columns = new FourColumnsTableFixture(
                connection,
                "Source4Cols",
                identityColumnIndex: 3
            );
            source4Columns.InsertTestData();
            var dest4Columns = new FourColumnsTableFixture(
                connection,
                "Destination4Cols",
                identityColumnIndex: 3
            );

            //Act
            DataFlowForIdentityColumn(connection);

            //Assert
            dest4Columns.AssertTestData();
        }
    }
}
