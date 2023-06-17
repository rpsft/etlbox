using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.CsvSource
{
    public class CsvSourceNoHeaderTests : FlatFileConnectorsTestBase
    {
        public CsvSourceNoHeaderTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        public class MySimpleRow
        {
            [Index(1)]
            public string Col2 { get; set; }

            [Index(0)]
            public int Col1 { get; set; }
        }

        [Fact]
        public void CsvSourceNoHeader()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CsvSourceNoHeader");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "CsvSourceNoHeader"
            );

            //Act
            CsvSource<MySimpleRow> source = new CsvSource<MySimpleRow>(
                "res/CsvSource/TwoColumnsNoHeader.csv"
            )
            {
                Configuration = { HasHeaderRecord = false }
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
