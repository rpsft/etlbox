using ALE.ETLBox.src.Definitions.DataFlow.Type;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestFlatFileConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestFlatFileConnectors.src.JsonSource
{
    public class JsonSourceTests : FlatFileConnectorsTestBase
    {
        public JsonSourceTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void JsonFromFile()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture("JsonSource2Cols");
            var dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "JsonSource2Cols"
            );

            //Act
            var source = new JsonSource<MySimpleRow>(
                "res/JsonSource/TwoColumns.json",
                ResourceType.File
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void ArrayInObject()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture(
                "JsonSourceArrayInObject"
            );
            var dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "JsonSourceArrayInObject"
            );

            //Act
            var source = new JsonSource<MySimpleRow>(
                "res/JsonSource/ArrayInObject.json",
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
