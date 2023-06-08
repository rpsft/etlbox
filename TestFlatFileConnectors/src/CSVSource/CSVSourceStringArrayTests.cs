using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.CSVSource
{
    [Collection("DataFlow")]
    public class CsvSourceStringArrayTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SimpleCSVIntoDatabase()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "CsvDestination2Columns"
            );
            DbDestination<string[]> dest = new DbDestination<string[]>(
                Connection,
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
                Connection,
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
                Connection,
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
                RowCountTask.Count(Connection, "CsvDestination2Columns", "Col1 IN (1,2,3)")
            );
        }
    }
}
