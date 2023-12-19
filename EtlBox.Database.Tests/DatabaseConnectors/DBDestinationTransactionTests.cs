using System.Data;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using EtlBox.Database.Tests.Infrastructure;
using EtlBox.Database.Tests.SharedFixtures;
using ETLBox.Primitives;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.DatabaseConnectors
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class DbDestinationTransactionTests : DatabaseTestBase
    {
        private readonly IConnectionManager _connection;

        protected DbDestinationTransactionTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
            _connection = _fixture.GetContainer(_connectionType).GetConnectionManager();
        }

        [Fact]
        public void ErrorInBatch()
        {
            //Arrange
            new TwoColumnsTableFixture(_connection, "TransactionDest");
            var source = new MemorySource<MySimpleRow>
            {
                DataAsList = new List<MySimpleRow>
                {
                    new() { Col1 = "1", Col2 = "Test1" },
                    new() { Col1 = "2", Col2 = "Test2" },
                    new() { Col1 = null, Col2 = "Test3" }
                }
            };
            var dest = new DbDestination<MySimpleRow>(_connection, "TransactionDest", 2);

            //Act & Assert
            source.LinkTo(dest);

            Assert.ThrowsAny<Exception>(() =>
            {
                source.Execute();
                dest.Wait();
            });

            //Assert
            Assert.Equal(2, RowCountTask.Count(_connection, "TransactionDest"));
            Assert.True(dest.BulkInsertConnectionManager.State == null);
            Assert.True(_connection.State == null);
        }

        [Fact]
        public void CloseConnectionDuringTransaction()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture(_connection, "TransactionSource");
            s2C.InsertTestData();
            var source = new DbSource<MySimpleRow>(_connection, "TransactionSource");

            var destinationConnection = _connection.Clone();
            new TwoColumnsTableFixture(destinationConnection, "TransactionDest");
            var dest = new DbDestination<MySimpleRow>(destinationConnection, "TransactionDest", 2);

            //Act & Assert
            destinationConnection.BeginTransaction();
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
            Assert.Equal(3, RowCountTask.Count(destinationConnection, "TransactionDest"));
            destinationConnection.Close();
            Assert.Equal(0, RowCountTask.Count(destinationConnection, "TransactionDest"));

            //Assert Connections are closed
            Assert.True(dest.BulkInsertConnectionManager.State == null);
            Assert.True(_connection.State == null);
            Assert.True(destinationConnection.State == null);
        }

        [Fact]
        public void CommitTransaction()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture(_connection, "TransactionSource");
            s2C.InsertTestData();
            new TwoColumnsTableFixture(_connection, "TransactionDest");
            var source = new DbSource<MySimpleRow>(_connection, "TransactionSource");
            var dest = new DbDestination<MySimpleRow>(_connection, "TransactionDest", 2);

            //Act & Assert
            _connection.BeginTransaction(IsolationLevel.ReadCommitted);
            source.LinkTo(dest);

            source.Execute();
            dest.Wait();

            //Assert
            if (_connection.GetType() == typeof(SqlConnectionManager))
                Assert.Equal(
                    3,
                    RowCountTask.Count(
                        _connection.Clone(),
                        "TransactionDest",
                        RowCountOptions.NoLock
                    )
                );
            _connection.CommitTransaction();
            Assert.Equal(3, RowCountTask.Count(_connection, "TransactionDest"));
            Assert.Equal(3, RowCountTask.Count(_connection.Clone(), "TransactionDest"));

            //Assert Connections are closed
            Assert.True(dest.BulkInsertConnectionManager.State == null);
            Assert.True(_connection.State == null);
        }

        [Fact]
        public void RollbackTransaction()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture(_connection, "TransactionSource");
            s2C.InsertTestData();
            var source = new DbSource<MySimpleRow>(_connection, "TransactionSource");

            var destinationConnection = _connection.Clone();
            new TwoColumnsTableFixture(destinationConnection, "TransactionDest");
            var dest = new DbDestination<MySimpleRow>(destinationConnection, "TransactionDest", 2);

            //Act & Assert
            destinationConnection.BeginTransaction(IsolationLevel.ReadCommitted);
            source.LinkTo(dest);

            source.Execute();
            dest.Wait();
            Assert.Equal(3, RowCountTask.Count(destinationConnection, "TransactionDest"));
            destinationConnection.RollbackTransaction();
            Assert.Equal(0, RowCountTask.Count(destinationConnection, "TransactionDest"));

            //Assert Connections are closed
            Assert.True(dest.BulkInsertConnectionManager.State == null);
            Assert.True(destinationConnection.State == null);
            Assert.True(_connection.State == null);
        }

        [Fact]
        public void LeaveOpen()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture(_connection, "TransactionSource");
            s2C.InsertTestData();
            new TwoColumnsTableFixture(_connection, "TransactionDest");
            var source = new DbSource<MySimpleRow>(_connection, "TransactionSource");
            var dest = new DbDestination<MySimpleRow>(_connection, "TransactionDest", 2);

            //Act
            _connection.LeaveOpen = true;
            _connection.BeginTransaction(IsolationLevel.ReadCommitted);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
            _connection.CommitTransaction();

            //Assert
            Assert.Equal(_connection, dest.BulkInsertConnectionManager);

            //Assert Connections are closed
            Assert.True(dest.BulkInsertConnectionManager.State == ConnectionState.Open);
            Assert.True(_connection.State == ConnectionState.Open);

            Assert.Equal(3, RowCountTask.Count(_connection, "TransactionDest"));
        }

        public class MySimpleRow
        {
            public string? Col1 { get; set; }
            public string? Col2 { get; set; }
        }

        public class SqlServer : DbDestinationTransactionTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : DbDestinationTransactionTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
