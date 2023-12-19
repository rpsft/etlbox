using System.Dynamic;
using ALE.ETLBox;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using EtlBox.Database.Tests.Infrastructure;
using EtlBox.Database.Tests.SharedFixtures;
using ETLBox.Primitives;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.DbDestinations.DatabaseConnectors
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class DbDestinationDynamicObjectTests : DatabaseTestBase
    {
        private readonly IConnectionManager _connection;

        protected DbDestinationDynamicObjectTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
            _connection = _fixture.GetConnectionManager(_connectionType);
        }

        [Fact]
        public void SourceMoreColumnsThanDestination()
        {
            //Arrange
            var source4Columns = new FourColumnsTableFixture(_connection, "SourceDynamic4Cols");
            source4Columns.InsertTestData();
            var dest2Columns = new TwoColumnsTableFixture(_connection, "DestinationDynamic2Cols");

            //Act
            var source = new DbSource<ExpandoObject>(_connection, "SourceDynamic4Cols");
            var dest = new DbDestination<ExpandoObject>(_connection, "DestinationDynamic2Cols");

            source.LinkTo(dest);
            source.Execute(CancellationToken.None);
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void DestinationMoreColumnsThanSource()
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(_connection, "SourceDynamicDiffCols");
            source2Columns.InsertTestData();
            CreateTableTask.Create(
                _connection,
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
            var source = new DbSource<ExpandoObject>(_connection, "SourceDynamicDiffCols");
            var dest = new DbDestination<ExpandoObject>(_connection, "DestinationDynamicDiffCols");

            source.LinkTo(dest);
            source.Execute(CancellationToken.None);
            dest.Wait();

            //Assert
            var qb = _connection.QB;
            var qe = _connection.QE;
            Assert.Equal(3, RowCountTask.Count(_connection, "DestinationDynamicDiffCols"));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    "DestinationDynamicDiffCols",
                    $"{qb}Col1{qe} = 1 AND {qb}Col2{qe}='Test1' AND {qb}Col5{qe} IS NULL AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    "DestinationDynamicDiffCols",
                    $"{qb}Col1{qe} = 2 AND {qb}Col2{qe}='Test2' AND {qb}Col5{qe} IS NULL AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    "DestinationDynamicDiffCols",
                    $"{qb}Col1{qe} = 3 AND {qb}Col2{qe}='Test3' AND {qb}Col5{qe} IS NULL AND {qb}ColX{qe} IS NULL"
                )
            );
        }

        [Fact]
        public void DestinationWithIdentityColumn()
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(_connection, "SourceDynamicIDCol");
            source2Columns.InsertTestData();
            CreateTableTask.Create(
                _connection,
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
            var source = new DbSource<ExpandoObject>(_connection, "SourceDynamicIDCol");
            var dest = new DbDestination<ExpandoObject>(_connection, "DestinationDynamicIdCol");

            source.LinkTo(dest);
            source.Execute(CancellationToken.None);
            dest.Wait();

            //Assert
            var qb = _connection.QB;
            var qe = _connection.QE;
            Assert.Equal(3, RowCountTask.Count(_connection, "DestinationDynamicIdCol"));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    "DestinationDynamicIdCol",
                    $"{qb}Col1{qe} = 1 AND {qb}Col2{qe}='Test1' AND {qb}Id{qe} > 0 AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    "DestinationDynamicIdCol",
                    $"{qb}Col1{qe} = 2 AND {qb}Col2{qe}='Test2' AND {qb}Id{qe} > 0 AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    "DestinationDynamicIdCol",
                    $"{qb}Col1{qe} = 3 AND {qb}Col2{qe}='Test3' AND {qb}Id{qe} > 0 AND {qb}ColX{qe} IS NULL"
                )
            );
        }

        public class SqlServer : DbDestinationDynamicObjectTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : DbDestinationDynamicObjectTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
