using System.Dynamic;
using System.Threading;
using ALE.ETLBox;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ETLBox.Primitives;

namespace TestDatabaseConnectors.DBDestination
{
    [Collection("DatabaseConnectors")]
    public class DbDestinationDynamicObjectTests : DatabaseConnectorsTestBase
    {
        public DbDestinationDynamicObjectTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        [Theory]
        [MemberData(nameof(ConnectionsWithoutClickHouse))]
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
            source.Execute(CancellationToken.None);
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Theory]
        [MemberData(nameof(AllSqlConnections))]
        public void DestinationMoreColumnsThanSource(IConnectionManager connection)
        {
            //Arrange
            DropTableTask.DropIfExists(connection, "DestinationDynamicDiffCols");
            var source2Columns = new TwoColumnsTableFixture(connection, "SourceDynamicDiffCols");
            source2Columns.InsertTestData();
            CreateTableTask.Create(
                connection,
                "DestinationDynamicDiffCols",
                new List<TableColumn>
                {
                    new("Id", "INT", false, true, true),
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
            var qb = connection.QB;
            var qe = connection.QE;
            Assert.Equal(3, RowCountTask.Count(connection, "DestinationDynamicDiffCols"));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DestinationDynamicDiffCols",
                    $"{qb}Col1{qe} = 1 AND {qb}Col2{qe}='Test1' AND {qb}Col5{qe} IS NULL AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DestinationDynamicDiffCols",
                    $"{qb}Col1{qe} = 2 AND {qb}Col2{qe}='Test2' AND {qb}Col5{qe} IS NULL AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DestinationDynamicDiffCols",
                    $"{qb}Col1{qe} = 3 AND {qb}Col2{qe}='Test3' AND {qb}Col5{qe} IS NULL AND {qb}ColX{qe} IS NULL"
                )
            );
        }

        [Theory]
        [MemberData(nameof(AllConnectionsWithoutSQLite))]
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
            var qb = connection.QB;
            var qe = connection.QE;
            Assert.Equal(3, RowCountTask.Count(connection, "DestinationDynamicIdCol"));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DestinationDynamicIdCol",
                    $"{qb}Col1{qe} = 1 AND {qb}Col2{qe}='Test1' AND {qb}Id{qe} > 0 AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DestinationDynamicIdCol",
                    $"{qb}Col1{qe} = 2 AND {qb}Col2{qe}='Test2' AND {qb}Id{qe} > 0 AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DestinationDynamicIdCol",
                    $"{qb}Col1{qe} = 3 AND {qb}Col2{qe}='Test3' AND {qb}Id{qe} > 0 AND {qb}ColX{qe} IS NULL"
                )
            );
        }
    }
}
