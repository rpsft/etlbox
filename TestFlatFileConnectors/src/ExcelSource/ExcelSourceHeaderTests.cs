using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class ExcelSourceHeaderTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public ExcelSourceHeaderTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
            public string ExtraColumn { get; set; }
        }

        [Fact]
        public void SimpleData()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("ExcelDestinationWithHeader");
            ExcelSource<MySimpleRow> source = new ExcelSource<MySimpleRow>("res/Excel/TwoColumnWithHeader.xlsx");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(Connection, "ExcelDestinationWithHeader");

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void MoreHeaderColumns()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("ExcelDestinationManyColsWithHeader");
            ExcelSource<MySimpleRow> source = new ExcelSource<MySimpleRow>("res/Excel/ManyColumnsWithHeader.xlsx");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(Connection, "ExcelDestinationManyColsWithHeader");

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

    }
}
