using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using EtlBox.Database.Tests.Infrastructure;
using EtlBox.Database.Tests.SharedFixtures;
using ETLBox.Primitives;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.DbDestinations.DatabaseConnectors
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class DbDestinationBatchChangesTests : DatabaseTestBase
    {
        private readonly IConnectionManager _connection;

        protected DbDestinationBatchChangesTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
            _connection = _fixture.GetConnectionManager(_connectionType);
        }

        [Fact]
        public void WithBatchChanges()
        {
            //Arrange
            var d2C = new TwoColumnsTableFixture(
                _connection,
                "DbDestinationBatchChanges"
            );
            var dest = new DbDestination<string[]>(
                _connection,
                "DbDestinationBatchChanges",
                batchSize: 2
            )
            {
                BeforeBatchWrite = rowArray =>
                {
                    rowArray[0][1] = "NewValue";
                    return rowArray;
                }
            };

            //Act
            var source = new CsvSource<string[]>("res/BatchChanges/TwoColumns.csv");
            source.LinkTo(dest);
            source.Execute(CancellationToken.None);
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(_connection, "DbDestinationBatchChanges"));
            Assert.Equal(
                2,
                RowCountTask.Count(
                    _connection,
                    "DbDestinationBatchChanges",
                    $"{d2C.QB}Col2{d2C.QE}='NewValue'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    _connection,
                    "DbDestinationBatchChanges",
                    $"{d2C.QB}Col1{d2C.QE} = 2 AND {d2C.QB}Col2{d2C.QE}='Test2'"
                )
            );
        }

        [Fact]
        public void AfterBatchWrite()
        {
            //Arrange
            var wasExecuted = false;
            new TwoColumnsTableFixture(_connection, "DbDestinationBatchChanges");
            var dest = new DbDestination<string[]>(
                _connection,
                "DbDestinationBatchChanges",
                batchSize: 1
            )
            {
                AfterBatchWrite = rowArray =>
                {
                    Assert.True(rowArray.Length == 1);
                    wasExecuted = true;
                }
            };

            //Act
            var source = new CsvSource<string[]>("res/BatchChanges/TwoColumns.csv");
            source.LinkTo(dest);
            source.Execute(CancellationToken.None);
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(_connection, "DbDestinationBatchChanges"));
            Assert.True(wasExecuted);
        }

        public class SqlServer : DbDestinationBatchChangesTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : DbDestinationBatchChangesTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
