using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.JsonSource
{
    public class JsonSourceErrorLinkingTests : FlatFileConnectorsTestBase
    {
        public JsonSourceErrorLinkingTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void WithObjectErrorLinking()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "JsonSourceErrorLinking"
            );
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "JsonSourceErrorLinking"
            );
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            JsonSource<MySimpleRow> source = new JsonSource<MySimpleRow>(
                "res/JsonSource/TwoColumnsErrorLinking.json",
                ResourceType.File
            );

            source.LinkTo(dest);
            source.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            dest2Columns.AssertTestData();
            Assert.Collection(
                errorDest.Data,
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    ),
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    )
            );
        }

        [Fact]
        public void WithoutErrorLinking()
        {
            //Arrange
            MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

            //Act
            JsonSource<MySimpleRow> source = new JsonSource<MySimpleRow>(
                "res/JsonSource/TwoColumnsErrorLinking.json",
                ResourceType.File
            );

            //Assert
            Assert.Throws<JsonReaderException>(() =>
            {
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }
    }
}
