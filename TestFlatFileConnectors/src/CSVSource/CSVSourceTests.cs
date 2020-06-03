using ETLBox.ConnectionManager;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using CsvHelper.Configuration.Attributes;
using Xunit;
using ETLBox.SqlServer;
using ETLBox.Csv;
using ETLBoxTests.Helper;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvSourceTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CsvSourceTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            [Name("Header2")]
            public string Col2 { get; set; }
            [Name("Header1")]
            public int Col1 { get; set; }
        }

        [Fact]
        public void SimpleFlowWithObject()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CsvSource2Cols");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(Connection, "CsvSource2Cols");

            //Act
            CsvSource<MySimpleRow> source = new CsvSource<MySimpleRow>("res/CsvSource/TwoColumns.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void CSVGenericWithSkipRows_DB()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CsvSourceSkipRows");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(Connection, "CsvSourceSkipRows");

            //Act
            CsvSource<MySimpleRow> source = new CsvSource<MySimpleRow>("res/CsvSource/TwoColumnsSkipRows.csv");
            source.SkipRows = 2;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
