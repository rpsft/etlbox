using System.Dynamic;
using ALE.ETLBox;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using EtlBox.Database.Tests.Infrastructure;
using EtlBox.Database.Tests.SharedFixtures;
using ETLBox.Primitives;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.DatabaseConnectors.DbTransformations
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class DbTransformationDynamicObjectTests : DatabaseTestBase
    {
        private readonly IConnectionManager _connection;

        protected DbTransformationDynamicObjectTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
            _connection = _fixture.GetConnectionManager(_connectionType);
        }

        [Fact]
        public void SourceMoreColumnsThanTransformation()
        {
            //Arrange
            var source4Columns = new FourColumnsTableFixture(_connection, "SourceDynamic4Cols");
            source4Columns.InsertTestData();
            var dest2Columns = new TwoColumnsTableFixture(_connection, "TransformationDynamic2Cols");

            //Act
            var source = new DbSource<ExpandoObject>(_connection, "SourceDynamic4Cols");
            var trans = new DbTransformation<ExpandoObject>(_connection, "TransformationDynamic2Cols");
            var dest = new MemoryDestination<ExpandoObject>();

            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
            Assert.Equal(3, dest.Data.Count);
        }

        [Fact]
        public void TransformationMoreColumnsThanSource()
        {
            //Arrange
            var sourceTable = "SourceDynamicDiffCols";
            var destTable = "TransformationDynamicDiffCols";
            var source2Columns = new TwoColumnsTableFixture(_connection, sourceTable);
            source2Columns.InsertTestData();
            CreateTableTask.Create(
                _connection,
                destTable,
                new List<TableColumn>
                {
                    new("Col5", "VARCHAR(100)", true),
                    new("Col2", "VARCHAR(100)", true),
                    new("Col1", "INT", true),
                    new("ColX", "INT", true)
                }
            );

            //Act
            var source = new DbSource<ExpandoObject>(_connection, sourceTable);
            var trans = new DbTransformation<ExpandoObject>(_connection, destTable);
            var dest = new MemoryDestination<ExpandoObject>();

            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            var qb = _connection.QB;
            var qe = _connection.QE;
            Assert.Equal(3, RowCountTask.Count(_connection, destTable));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    destTable,
                    $"{qb}Col1{qe} = 1 AND {qb}Col2{qe}='Test1' AND {qb}Col5{qe} IS NULL AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    destTable,
                    $"{qb}Col1{qe} = 2 AND {qb}Col2{qe}='Test2' AND {qb}Col5{qe} IS NULL AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    destTable,
                    $"{qb}Col1{qe} = 3 AND {qb}Col2{qe}='Test3' AND {qb}Col5{qe} IS NULL AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(3, dest.Data.Count);
        }

        [Fact]
        public void TransformationWithIdentityColumn()
        {
            //Arrange
            var sourceTable = "SourceDynamicIDCol";
            var destTable = "TransformationDynamicIdCol";
            var source2Columns = new TwoColumnsTableFixture(_connection, sourceTable);
            source2Columns.InsertTestData();
            CreateTableTask.Create(
                _connection,
                destTable,
                new List<TableColumn>
                {
                    new("Id", "BIGINT", false, true, true),
                    new("Col2", "VARCHAR(100)", true),
                    new("Col1", "INT", true),
                    new("ColX", "INT", true)
                }
            );

            //Act
            var source = new DbSource<ExpandoObject>(_connection, sourceTable);
            var trans = new DbTransformation<ExpandoObject>(_connection, destTable);
            var dest = new MemoryDestination<ExpandoObject>();

            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            var qb = _connection.QB;
            var qe = _connection.QE;
            Assert.Equal(3, RowCountTask.Count(_connection, destTable));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    destTable,
                    $"{qb}Col1{qe} = 1 AND {qb}Col2{qe}='Test1' AND {qb}Id{qe} > 0 AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    destTable,
                    $"{qb}Col1{qe} = 2 AND {qb}Col2{qe}='Test2' AND {qb}Id{qe} > 0 AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    destTable,
                    $"{qb}Col1{qe} = 3 AND {qb}Col2{qe}='Test3' AND {qb}Id{qe} > 0 AND {qb}ColX{qe} IS NULL"
                )
            );
            Assert.Equal(3, dest.Data.Count);
        }

        public class SqlServer : DbTransformationDynamicObjectTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : DbTransformationDynamicObjectTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
