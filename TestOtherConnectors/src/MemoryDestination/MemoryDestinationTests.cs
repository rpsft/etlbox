using ALE.ETLBox.src.Toolbox.DataFlow;
using TestOtherConnectors.src;
using TestOtherConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestOtherConnectors.src.MemoryDestination
{
    public class MemoryDestinationTests : OtherConnectorsTestBase
    {
        public MemoryDestinationTests(OtherConnectorsDatabaseFixture fixture)
            : base(fixture) { }

        [Serializable]
        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void DataIsInList()
        {
            //Arrange
            var source2Columns = new TwoColumnsTableFixture(
                "MemoryDestinationSource"
            );
            source2Columns.InsertTestData();

            var source = new DbSource<MySimpleRow>(
                SqlConnection,
                "MemoryDestinationSource"
            );
            var dest = new MemoryDestination<MySimpleRow>();

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection(
                dest.Data,
                d => Assert.True(d.Col1 == 1 && d.Col2 == "Test1"),
                d => Assert.True(d.Col1 == 2 && d.Col2 == "Test2"),
                d => Assert.True(d.Col1 == 3 && d.Col2 == "Test3")
            );
        }
    }
}
