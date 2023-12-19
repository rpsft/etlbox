using ALE.ETLBox.DataFlow;
using TestFlatFileConnectors.Fixture;
using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.ExcelSource
{
    public class ExcelSourceErrorLinkingTests : FlatFileConnectorsTestBase
    {
        public ExcelSourceErrorLinkingTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        public class MySimpleRow
        {
            [ExcelColumn(0)]
            public int Col1 { get; set; }

            [ExcelColumn(1)]
            public string Col2 { get; set; }
        }

        [Fact]
        public void WithObjectErrorLinking()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture(
                "ExcelSourceErrorLinking"
            );
            var dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "ExcelSourceErrorLinking"
            );
            var errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            var source = new ExcelSource<MySimpleRow>(
                "res/Excel/TwoColumnErrorLinking.xlsx"
            )
            {
                HasNoHeader = true
            };
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
                    )
            );
        }

        [Fact]
        public void WithoutErrorLinking()
        {
            //Arrange
            var source = new ExcelSource<MySimpleRow>(
                "res/Excel/TwoColumnErrorLinking.xlsx"
            )
            {
                HasNoHeader = true
            };
            var dest = new MemoryDestination<MySimpleRow>();

            //Act & Assert
            Assert.Throws<FormatException>(() =>
            {
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }
    }
}
