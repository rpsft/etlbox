using System.Dynamic;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestOtherConnectors.src;
using TestOtherConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestOtherConnectors.src.MemoryDestination
{
    public class MemoryDestinationDynamicObjectTests : OtherConnectorsTestBase
    {
        public MemoryDestinationDynamicObjectTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void DataIsInList()
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(
                "MemoryDestinationSource"
            );
            source2Columns.InsertTestData();

            var source = new DbSource<ExpandoObject>(
                SqlConnection,
                "MemoryDestinationSource"
            );
            var dest = new MemoryDestination<ExpandoObject>();

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            var index = 1;
            foreach (dynamic d in dest.Data)
            {
                Assert.True(d.Col1 == index && d.Col2 == "Test" + index);
                index++;
            }
        }
    }
}
