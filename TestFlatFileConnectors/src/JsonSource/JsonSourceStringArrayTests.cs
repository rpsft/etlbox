using ALE.ETLBox.src.Definitions.DataFlow.Type;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestFlatFileConnectors.src;
using TestFlatFileConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestFlatFileConnectors.src.JsonSource
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
