using ALE.ETLBox.Common.DataFlow;
using ALE.ETLBox.DataFlow;
using TestFlatFileConnectors.Fixture;
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
            var dest2Columns = new TwoColumnsTableFixture(
                "JsonSource2ColsDynamic"
            );
            var trans = new RowTransformation<ExpandoObject>(row =>
            {
                dynamic r = row;
                r.Col1 = r.Column1;
                r.Col2 = r.Column2;
                return r;
            });
            var dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "JsonSource2ColsDynamic"
            );

            //Act
            var source = new JsonSource<ExpandoObject>(
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
