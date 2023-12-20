using ALE.ETLBox.DataFlow;
using EtlBox.Database.Tests.Infrastructure;
using EtlBox.Database.Tests.SharedFixtures;
using ETLBox.Primitives;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.DatabaseConnectors.DbDestinations
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class DbDestinationErrorLinkingTests : DatabaseTestBase
    {
        private readonly IConnectionManager connection;

        protected DbDestinationErrorLinkingTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
            connection = _fixture.GetConnectionManager(_connectionType);
        }

        [Fact]
        public void RedirectBatch()
        {
            //Arrange
            var d2C = new TwoColumnsTableFixture(connection, "DestLinkError");
            var source = new MemorySource<MySimpleRow>
            {
                DataAsList = new List<MySimpleRow>
                {
                    new() { Col1 = null, Col2 = "ErrorRecord" },
                    new() { Col1 = "X2", Col2 = "ErrorRecord" },
                    new() { Col1 = "1", Col2 = "Test1" },
                    new() { Col1 = "2", Col2 = "Test2" },
                    new() { Col1 = "3", Col2 = "Test3 - good, but in error batch" },
                    new() { Col1 = null, Col2 = "ErrorRecord" },
                    new() { Col1 = "3", Col2 = "Test3" }
                }
            };
            var dest = new DbDestination<MySimpleRow>(
                connection,
                "DestLinkError",
                batchSize: 2
            );
            var errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            source.LinkTo(dest);
            dest.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            d2C.AssertTestData();
            Assert.Collection(
                errorDest.Data,
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    ),
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    )
            );
        }

        public class MySimpleRow
        {
            public string? Col1 { get; set; }
            public string? Col2 { get; set; }
        }

        public class SqlServer : DbDestinationErrorLinkingTests
        {
            public SqlServer(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : DbDestinationErrorLinkingTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger) : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
