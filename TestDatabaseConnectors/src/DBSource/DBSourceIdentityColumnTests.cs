using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBSource
{
    [Collection("DataFlow")]
    public class DbSourceIdentityColumnTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public class MyPartialRow
        {
            public string Col2 { get; set; }
            public decimal? Col4 { get; set; }
        }

        private void DataFlowForIdentityColumn(IConnectionManager connection)
        {
            DbSource<MyPartialRow> source = new DbSource<MyPartialRow>(connection, "Source4Cols");
            DbDestination<MyPartialRow> dest = new DbDestination<MyPartialRow>(
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
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture(
                connection,
                "Source4Cols",
                identityColumnIndex: 0
            );
            source4Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(
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
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture(
                connection,
                "Source4Cols",
                identityColumnIndex: 1
            );
            source4Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(
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
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture(
                connection,
                "Source4Cols",
                identityColumnIndex: 3
            );
            source4Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(
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
