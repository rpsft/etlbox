using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.DataFlow;
using EtlBox.Database.Tests.Infrastructure;
using EtlBox.Database.Tests.SharedFixtures;
using ETLBox.Primitives;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.DatabaseConnectors
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class DbDestinationTests : DatabaseTestBase
    {
        private readonly IConnectionManager _connection;

        protected DbDestinationTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
            _connection = _fixture.GetConnectionManager(_connectionType);
        }

        [Fact]
        public void ColumnMapping()
        {
            //Arrange
            var source4Columns = new FourColumnsTableFixture(
                _connection,
                "Source"
            );
            source4Columns.InsertTestData();
            var dest4Columns = new FourColumnsTableFixture(
                _connection,
                "Destination",
                identityColumnIndex: 2
            );

            var source = new DbSource<string[]>(_connection, "Source");
            var trans = new RowTransformation<
                string[],
                MyExtendedRow
            >(
                row =>
                    new MyExtendedRow
                    {
                        Id = int.Parse(row[0]),
                        Text = row[1],
                        Value = row[2] != null ? long.Parse(row[2]) : null,
                        Percentage = decimal.Parse(row[3])
                    }
            );

            //Act
            var dest = new DbDestination<MyExtendedRow>(
                _connection,
                "Destination"
            );
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute(CancellationToken.None);
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
        }

        public class MyExtendedRow
        {
            [ColumnMap("Col1")]
            public int Id { get; set; }

            [ColumnMap("Col3")]
            public long? Value { get; set; }

            [ColumnMap("Col4")]
            public decimal Percentage { get; set; }

            [ColumnMap("Col2")]
            public string? Text { get; set; }
        }

        public class SqlServer : DbDestinationTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : DbDestinationTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
