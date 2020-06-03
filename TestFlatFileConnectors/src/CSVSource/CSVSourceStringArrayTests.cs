using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.Csv;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBox.SqlServer;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvSourceStringArrayTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CsvSourceStringArrayTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SimpleCSVIntoDatabase()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CsvDestination2Columns");
            DbDestination<string[]> dest = new DbDestination<string[]>(Connection, "CsvDestination2Columns");

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
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CsvDestination2Columns");
            DbDestination<string[]> dest = new DbDestination<string[]>(Connection, "CsvDestination2Columns");

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
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CsvDestination2Columns");
            DbDestination<string[]> dest = new DbDestination<string[]>(Connection, "CsvDestination2Columns");

            //Act
            CsvSource<string[]> source = new CsvSource<string[]>("res/CsvSource/OneColumn.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(Connection, "CsvDestination2Columns", "Col1 IN (1,2,3)"));
        }
    }
}
