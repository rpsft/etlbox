using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.DataFlow;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestDatabaseConnectors.src.Fixtures;
using TestShared.src.SharedFixtures;

namespace TestDatabaseConnectors.src.DBDestination
{
    public class DbDestinationErrorLinkingTests : DatabaseConnectorsTestBase
    {
        public DbDestinationErrorLinkingTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        public class MySimpleRow
        {
            public string Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void RedirectBatch(IConnectionManager connection)
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
    }
}
