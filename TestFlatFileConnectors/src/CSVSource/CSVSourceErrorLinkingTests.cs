using CsvHelper.TypeConversion;
using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.CSVSource
{
    public class CsvSourceErrorLinkingTests : FlatFileConnectorsTestBase
    {
        public CsvSourceErrorLinkingTests(FlatFileToDatabaseFixture fixture)
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
                "CsvSourceErrorLinking"
            );
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "CsvSourceErrorLinking"
            );
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            CsvSource<MySimpleRow> source = new CsvSource<MySimpleRow>(
                "res/CsvSource/TwoColumnsErrorLinking.csv"
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
                    ),
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
            CsvSource<MySimpleRow> source = new CsvSource<MySimpleRow>(
                "res/CsvSource/TwoColumnsErrorLinking.csv"
            );

            //Assert
            Assert.Throws<TypeConverterException>(() =>
            {
                source.LinkTo(dest);
                source.Execute();
                dest.Wait();
            });
        }
    }
}
