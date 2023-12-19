using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using EtlBox.Database.Tests.Infrastructure;
using EtlBox.Database.Tests.SharedFixtures;
using ETLBox.Primitives;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.DbDestinations.DatabaseConnectors
{
    [Collection(nameof(DatabaseCollection))]

    public abstract class DbDestinationStringArrayTests : DatabaseTestBase
    {
        private readonly IConnectionManager _connection;

        protected DbDestinationStringArrayTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
            _connection = _fixture.GetConnectionManager(_connectionType);
        }

        [Fact]
        public void WithSqlNotMatchingColumns()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture(
                _connection,
                "SourceNotMatchingCols"
            );
            s2C.InsertTestData();
            SqlTask.ExecuteNonQuery(
                _connection,
                "Create destination table",
                $@"CREATE TABLE destination_notmatchingcols
                ( col3 VARCHAR(100) NULL
                , col4 VARCHAR(100) NULL
                , {s2C.QB}Col1{s2C.QE} VARCHAR(100) NULL)"
            );

            //Act
            var source = new DbSource<string[]>
            {
                Sql =
                    $"SELECT {s2C.QB}Col1{s2C.QE}, {s2C.QB}Col2{s2C.QE} FROM {s2C.QB}SourceNotMatchingCols{s2C.QE}",
                ConnectionManager = _connection
            };
            var dest = new DbDestination<string[]>(
                _connection,
                "destination_notmatchingcols"
            );
            source.LinkTo(dest);
            source.Execute(CancellationToken.None);
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(_connection, "destination_notmatchingcols"));
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    "destination_notmatchingcols",
                    "col3 = '1' AND col4='Test1'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    "destination_notmatchingcols",
                    "col3 = '2' AND col4='Test2'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    "destination_notmatchingcols",
                    "col3 = '3' AND col4='Test3'"
                )
            );
        }

        [Fact]
        public void WithLessColumnsInDestination()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture(_connection, "SourceTwoColumns");
            s2C.InsertTestData();
            SqlTask.ExecuteNonQuery(
                _connection,
                "Create destination table",
                @"CREATE TABLE destination_onecolumn
                (colx varchar (100) not null )"
            );

            //Act
            var source = new DbSource<string[]>(_connection, "SourceTwoColumns");
            var dest = new DbDestination<string[]>(
                _connection,
                "destination_onecolumn"
            );
            source.LinkTo(dest);
            source.Execute(CancellationToken.None);
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(_connection, "destination_onecolumn"));
            Assert.Equal(1, RowCountTask.Count(_connection, "destination_onecolumn", "colx = '1'"));
            Assert.Equal(1, RowCountTask.Count(_connection, "destination_onecolumn", "colx = '2'"));
            Assert.Equal(1, RowCountTask.Count(_connection, "destination_onecolumn", "colx = '3'"));
        }

        [Fact]
        public void WithAdditionalNullableCol()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture(
                _connection,
                "source_additionalnullcol"
            );
            s2C.InsertTestData();
            SqlTask.ExecuteNonQuery(
                _connection,
                "Create destination table",
                @"CREATE TABLE destination_additionalnullcol
                (col1 VARCHAR(100) NULL, col2 VARCHAR(100) NULL, col3 VARCHAR(100) NULL)"
            );

            //Act
            var source = new DbSource<string[]>(
                _connection,
                "source_additionalnullcol"
            );
            var dest = new DbDestination<string[]>(
                _connection,
                "destination_additionalnullcol"
            );
            source.LinkTo(dest);
            source.Execute(CancellationToken.None);
            dest.Wait();

            //Assert
            s2C.AssertTestData();
        }

        [Fact]
        public void WithAdditionalNotNullCol()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture(
                _connection,
                "source_additionalnotnullcol"
            );
            s2C.InsertTestData();
            SqlTask.ExecuteNonQuery(
                _connection,
                "Create destination table",
                @"CREATE TABLE destination_additionalnotnullcol
                (col1 VARCHAR(100) NULL, col2 VARCHAR(100) NULL, col3 VARCHAR(100) NOT NULL)"
            );

            //Act
            var source = new DbSource<string[]>(
                _connection,
                "source_additionalnotnullcol"
            );
            var dest = new DbDestination<string[]>(
                _connection,
                "destination_additionalnotnullcol"
            );
            source.LinkTo(dest);
            Assert.Throws<AggregateException>(() =>
            {
                source.Execute(CancellationToken.None);
                dest.Wait();
            });
        }

        public class SqlServer : DbDestinationStringArrayTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : DbDestinationStringArrayTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
