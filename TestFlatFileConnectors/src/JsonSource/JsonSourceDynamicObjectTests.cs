using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.JsonSource
{
    public class JsonSourceDynamicObjectTests : FlatFileConnectorsTestBase
    {
        public JsonSourceDynamicObjectTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void SourceWithDifferentNames()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "JsonSource2ColsDynamic"
            );
            RowTransformation<ExpandoObject> trans = new RowTransformation<ExpandoObject>(row =>
            {
                dynamic r = row;
                r.Col1 = r.Column1;
                r.Col2 = r.Column2;
                return r;
            });
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "JsonSource2ColsDynamic"
            );

            //Act
            JsonSource<ExpandoObject> source = new JsonSource<ExpandoObject>(
                "res/JsonSource/TwoColumnsDifferentNames.json",
                ResourceType.File
            );
            source.LinkTo(trans).LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
