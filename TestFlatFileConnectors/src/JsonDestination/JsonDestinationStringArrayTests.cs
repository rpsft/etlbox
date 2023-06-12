using TestFlatFileConnectors.Helpers;
using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.JsonDestination
{
    public class JsonDestinationStringArrayTests : FlatFileConnectorsTestBase
    {
        public JsonDestinationStringArrayTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void SimpleNonGeneric()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("JsonDestSimpleNonGeneric");
            s2C.InsertTestDataSet3();
            DbSource<string[]> source = new DbSource<string[]>(
                SqlConnection,
                "JsonDestSimpleNonGeneric"
            );

            //Act
            JsonDestination<string[]> dest = new JsonDestination<string[]>(
                "./SimpleNonGeneric.json",
                ResourceType.File
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                File.ReadAllText("res/JsonDestination/TwoColumnsSet3StringArray.json")
                    .NormalizeLineEndings(),
                File.ReadAllText("./SimpleNonGeneric.json")
            );
        }
    }
}
