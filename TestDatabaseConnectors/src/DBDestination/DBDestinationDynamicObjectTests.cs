using System.Dynamic;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBDestination
{
    [Collection("DataFlow")]
    public class DbDestinationDynamicObjectTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public static IEnumerable<object[]> ConnectionsNoSQLite =>
            Config.AllConnectionsWithoutSQLite("DataFlow");

        [Theory]
        [MemberData(nameof(Connections))]
        public void SourceMoreColumnsThanDestination(IConnectionManager connection)
        {
            //Arrange
            var source4Columns = new FourColumnsTableFixture(connection, "SourceDynamic4Cols");
            source4Columns.InsertTestData();
            var dest2Columns = new TwoColumnsTableFixture(connection, "DestinationDynamic2Cols");

            //Act
            var source = new DbSource<ExpandoObject>(connection, "SourceDynamic4Cols");
            var dest = new DbDestination<ExpandoObject>(connection, "DestinationDynamic2Cols");

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Theory]
        [MemberData(nameof(Connections))]
        public void DestinationMoreColumnsThanSource(IConnectionManager connection)
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(connection, "SourceDynamicDiffCols");
            source2Columns.InsertTestData();
            CreateTableTask.Create(
                connection,
                "DestinationDynamicDiffCols",
                new List<TableColumn>
                {
                    new("Col5", "VARCHAR(100)", true),
                    new("Col2", "VARCHAR(100)", true),
                    new("Col1", "INT", true),
                    new("ColX", "INT", true)
                }
            );

            //Act
            var source = new DbSource<ExpandoObject>(connection, "SourceDynamicDiffCols");
            var dest = new DbDestination<ExpandoObject>(connection, "DestinationDynamicDiffCols");

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            var QB = connection.QB;
            var QE = connection.QE;
            Assert.Equal(3, RowCountTask.Count(connection, "DestinationDynamicDiffCols"));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DestinationDynamicDiffCols",
                    $"{QB}Col1{QE} = 1 AND {QB}Col2{QE}='Test1' AND {QB}Col5{QE} IS NULL AND {QB}ColX{QE} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DestinationDynamicDiffCols",
                    $"{QB}Col1{QE} = 2 AND {QB}Col2{QE}='Test2' AND {QB}Col5{QE} IS NULL AND {QB}ColX{QE} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DestinationDynamicDiffCols",
                    $"{QB}Col1{QE} = 3 AND {QB}Col2{QE}='Test3' AND {QB}Col5{QE} IS NULL AND {QB}ColX{QE} IS NULL"
                )
            );
        }

        [Theory]
        [MemberData(nameof(ConnectionsNoSQLite))]
        public void DestinationWithIdentityColumn(IConnectionManager connection)
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(connection, "SourceDynamicIDCol");
            source2Columns.InsertTestData();
            CreateTableTask.Create(
                connection,
                "DestinationDynamicIdCol",
                new List<TableColumn>
                {
                    new("Id", "BIGINT", false, true, true),
                    new("Col2", "VARCHAR(100)", true),
                    new("Col1", "INT", true),
                    new("ColX", "INT", true)
                }
            );

            //Act
            var source = new DbSource<ExpandoObject>(connection, "SourceDynamicIDCol");
            var dest = new DbDestination<ExpandoObject>(connection, "DestinationDynamicIdCol");

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            var QB = connection.QB;
            var QE = connection.QE;
            Assert.Equal(3, RowCountTask.Count(connection, "DestinationDynamicIdCol"));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DestinationDynamicIdCol",
                    $"{QB}Col1{QE} = 1 AND {QB}Col2{QE}='Test1' AND {QB}Id{QE} > 0 AND {QB}ColX{QE} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DestinationDynamicIdCol",
                    $"{QB}Col1{QE} = 2 AND {QB}Col2{QE}='Test2' AND {QB}Id{QE} > 0 AND {QB}ColX{QE} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DestinationDynamicIdCol",
                    $"{QB}Col1{QE} = 3 AND {QB}Col2{QE}='Test3' AND {QB}Id{QE} > 0 AND {QB}ColX{QE} IS NULL"
                )
            );
        }
    }
}
