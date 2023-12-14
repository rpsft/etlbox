using ALE.ETLBox.src.Definitions.DataFlow;
using ALE.ETLBox.src.Definitions.DataFlow.Type;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestFlatFileConnectors.src;
using TestFlatFileConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestFlatFileConnectors.src.JsonSource
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
            var dest2Columns = new TwoColumnsTableFixture(
                "JsonSourceErrorLinking"
            );
            var dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "JsonSourceErrorLinking"
            );
            var errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            var source = new JsonSource<MySimpleRow>(
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
            var dest = new MemoryDestination<MySimpleRow>();

            //Act
            var source = new JsonSource<MySimpleRow>(
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
