using ALE.ETLBox;
using ALE.ETLBox.DataFlow;
using EtlBox.Database.Tests.Infrastructure;
using ETLBox.Primitives;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.DbDestinations.DatabaseConnectors
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class DbDestinationExceptionTests : DatabaseTestBase
    {
        private readonly IConnectionManager _connection;

        protected DbDestinationExceptionTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
            _connection = _fixture.GetConnectionManager(_connectionType);
        }

        [Fact]
        public void UnknownTable()
        {
            //Arrange
            string[] data = { "1", "2" };
            var source = new MemorySource<string[]>();
            source.DataAsList.Add(data);
            var dest = new DbDestination<string[]>(
                _connection,
                "UnknownTable"
            );
            source.LinkTo(dest);

            //Act & Assert
            Assert.Throws<InvalidOperationException>(() =>
            {
                try
                {
                    source.Execute(CancellationToken.None);
                    dest.Wait();
                }
                catch (AggregateException e)
                {
                    throw e.InnerException!;
                }
            });
        }

        [Fact]
        public void UnknownTableViaTableDefinition()
        {
            //Arrange
            var def = new TableDefinition(
                "UnknownTable",
                new List<TableColumn> { new("id", "INT") }
            );

            //Arrange
            string[] data = { "1", "2" };
            var source = new MemorySource<string[]>();
            source.DataAsList.Add(data);
            var dest = new DbDestination<string[]>
            {
                ConnectionManager = _connection,
                DestinationTableDefinition = def
            };
            source.LinkTo(dest);

            //Act & Assert
            Assert.Throws<InvalidOperationException>(() =>
            {
                try
                {
                    source.Execute(CancellationToken.None);
                    dest.Wait();
                }
                catch (AggregateException e)
                {
                    throw e.InnerException!;
                }
            });
        }

        public class SqlServer : DbDestinationExceptionTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : DbDestinationExceptionTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
