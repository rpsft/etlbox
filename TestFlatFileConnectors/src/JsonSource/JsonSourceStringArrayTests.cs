using ALE.ETLBox.DataFlow;
using TestFlatFileConnectors.Fixture;
using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.JsonSource
{
    public class JsonSourceStringArrayTests : FlatFileConnectorsTestBase
    {
        public JsonSourceStringArrayTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void SimpleFlowWithStringArray()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture(
                "JsonSource2ColsNonGen"
            );
            var dest = new DbDestination<string[]>(
                SqlConnection,
                "JsonSource2ColsNonGen"
            );

            //Act
            var source = new JsonSource<string[]>(
                "res/JsonSource/TwoColumnsStringArray.json",
                ResourceType.File
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
