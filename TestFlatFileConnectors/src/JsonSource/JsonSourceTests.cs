using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.JsonSource
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
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("JsonSource2Cols");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "JsonSource2Cols"
            );

            //Act
            JsonSource<MySimpleRow> source = new JsonSource<MySimpleRow>(
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
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "JsonSourceArrayInObject"
            );
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "JsonSourceArrayInObject"
            );

            //Act
            JsonSource<MySimpleRow> source = new JsonSource<MySimpleRow>(
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