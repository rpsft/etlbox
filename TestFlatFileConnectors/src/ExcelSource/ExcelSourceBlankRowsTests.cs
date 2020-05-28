using ETLBox.ConnectionManager;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class ExcelSourceBlankRowsTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public ExcelSourceBlankRowsTests()
        {
        }

        public class MyDataRow
        {
            [ExcelColumn(0)]
            public int No { get; set; }
            [ExcelColumn(1)]
            public string ID { get; set; }
            [ExcelColumn(2)]
            public string Desc { get; set; }
        }

        [Fact]
        public void ExcelNoBlankRows()
        {
            //Arrange
            //Act
            var result = LoadExcelIntoMemory("res/Excel/DemoExcel_BlankRows_OK.xlsx");
            //Assert
            Assert.True(result.Count == 6);
        }

        [Fact]
        public void ExcelWithBlankRows()
        {
            //Arrange
            //Act
            var result = LoadExcelIntoMemory("res/Excel/DemoExcel_BlankRows_Error.xlsx");
            //Assert
            Assert.True(result.Count == 6);
        }

        private IList<MyDataRow> LoadExcelIntoMemory(string filename)
        {
            MemoryDestination<MyDataRow> dest = new MemoryDestination<MyDataRow>();

            ExcelSource<MyDataRow> source = new ExcelSource<MyDataRow>(filename)
            {
                Range = new ExcelRange(1, 3),
                HasNoHeader = true
            };

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            return dest.Data.ToList();
        }

        public class MySimpleRow
        {
            [ExcelColumn(0)]
            public int Col1 { get; set; }
            [ExcelColumn(1)]
            public string Col2 { get; set; }
        }

        [Fact]
        public void IgnoreBlankRows()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("ExcelDestinationBlankRows");

            //Act
            ExcelSource<MySimpleRow> source = new ExcelSource<MySimpleRow>("res/Excel/TwoColumnBlankRow.xlsx")
            {
                IgnoreBlankRows = true,
                HasNoHeader = true
            };
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(Connection, "ExcelDestinationBlankRows", 2);

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

    }
}
