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
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "JsonSource2ColsNonGen"
            );
            DbDestination<string[]> dest = new DbDestination<string[]>(
                SqlConnection,
                "JsonSource2ColsNonGen"
            );

            //Act
            JsonSource<string[]> source = new JsonSource<string[]>(
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
