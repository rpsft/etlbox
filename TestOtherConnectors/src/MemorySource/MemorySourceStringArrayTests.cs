using ALE.ETLBox.DataFlow;
using TestOtherConnectors.Fixture;
using TestShared.SharedFixtures;

namespace TestOtherConnectors.MemorySource
{
    public class MemorySourceStringArrayTests : OtherConnectorsTestBase
    {
        public MemorySourceStringArrayTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void DataIsFromList()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture(
                "MemoryDestinationNonGeneric"
            );
            var source = new MemorySource<string[]>();
            var dest = new DbDestination<string[]>(
                SqlConnection,
                "MemoryDestinationNonGeneric"
            );

            //Act
            source.DataAsList = new List<string[]>
            {
                new[] { "1", "Test1" },
                new[] { "2", "Test2" },
                new[] { "3", "Test3" }
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
