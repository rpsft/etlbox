using ALE.ETLBox.src.Toolbox.DataFlow;
using TestOtherConnectors.src;
using TestOtherConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestOtherConnectors.src.MemoryDestination
{
    public class MemoryDestinationStringArrayTests : OtherConnectorsTestBase
    {
        public MemoryDestinationStringArrayTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void DataIsInList()
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(
                "MemoryDestinationNonGenericSource"
            );
            source2Columns.InsertTestData();

            var source = new DbSource<string[]>(
                SqlConnection,
                "MemoryDestinationNonGenericSource"
            );
            var dest = new MemoryDestination<string[]>();

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(
                dest.Data,
                d => Assert.True(d[0] == "1" && d[1] == "Test1"),
                d => Assert.True(d[0] == "2" && d[1] == "Test2"),
                d => Assert.True(d[0] == "3" && d[1] == "Test3")
            );
        }
    }
}
