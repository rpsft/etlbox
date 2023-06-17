using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.CsvSource
{
    public class CsvSourceStringArrayTests : FlatFileConnectorsTestBase
    {
        public CsvSourceStringArrayTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void SimpleCSVIntoDatabase()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "CsvDestination2Columns"
            );
            DbDestination<string[]> dest = new DbDestination<string[]>(
                SqlConnection,
                "CsvDestination2Columns"
            );

            //Act
            CsvSource<string[]> source = new CsvSource<string[]>("res/CsvSource/TwoColumns.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void MoreColumnsInCSV()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "CsvDestination2Columns"
            );
            DbDestination<string[]> dest = new DbDestination<string[]>(
                SqlConnection,
                "CsvDestination2Columns"
            );

            //Act
            CsvSource<string[]> source = new CsvSource<string[]>("res/CsvSource/ThreeColumns.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void MoreColumnsInDatabase()
        {
            //Arrange
            var unused = new TwoColumnsTableFixture("CsvDestination2Columns");
            DbDestination<string[]> dest = new DbDestination<string[]>(
                SqlConnection,
                "CsvDestination2Columns"
            );

            //Act
            CsvSource<string[]> source = new CsvSource<string[]>("res/CsvSource/OneColumn.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                3,
                RowCountTask.Count(SqlConnection, "CsvDestination2Columns", "Col1 IN (1,2,3)")
            );
        }
    }
}
