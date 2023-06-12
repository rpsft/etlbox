using TestFlatFileConnectors.Helpers;
using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.JsonDestination
{
    public class JsonDestinationDynamicObjectTests : FlatFileConnectorsTestBase
    {
        public JsonDestinationDynamicObjectTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void SimpleFlowWithObject()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture("JsonDestDynamic");
            s2C.InsertTestDataSet3();
            var source = new DbSource<ExpandoObject>(SqlConnection, "JsonDestDynamic");

            //Act
            var dest = new JsonDestination<ExpandoObject>(
                "./SimpleWithDynamicObject.json",
                ResourceType.File
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            //Null values can't be ignored:
            //https://github.com/JamesNK/Newtonsoft.Json/issues/1466
            Assert.Equal(
                File.ReadAllText("res/JsonDestination/TwoColumnsSet3DynamicObject.json")
                    .NormalizeLineEndings(),
                File.ReadAllText("./SimpleWithDynamicObject.json")
            );
        }
    }
}
