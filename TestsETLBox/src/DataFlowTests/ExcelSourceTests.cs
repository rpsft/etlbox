using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class ExcelSourceTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public ExcelSourceTests(DatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class MySimpleRow
        {
            [ExcelColumn(0)]
            public int Col1 { get; set; }
            [ExcelColumn(1)]
            public string Col2 { get; set; }
        }

        [Fact]
        public void SimpleData()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("ExcelDestination");

            //Act
            ExcelSource<MySimpleRow> source = new ExcelSource<MySimpleRow>("res/Excel/TwoColumnData.xlsx");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(Connection, "ExcelDestination", 2);

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        public class OneExcelColumn
        {
            public int Col1 { get; set; }
            [ExcelColumn(1)]
            public string Col2 { get; set; }
        }

        [Fact]
        public void OnlyOneExcelColumn()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("ExcelDestination");

            //Act
            ExcelSource<OneExcelColumn> source = new ExcelSource<OneExcelColumn>("res/Excel/TwoColumnData.xlsx");
            DBDestination<OneExcelColumn> dest = new DBDestination<OneExcelColumn>(Connection, "ExcelDestination", 2);

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(Connection, "ExcelDestination", "Col1 = 0 AND Col2 LIKE 'Test%'"));
        }

        public class ExcelDataSheet2
        {
            [ExcelColumn(1)]
            public string Col2 { get; set; }
            [ExcelColumn(2)]
            public decimal? Col4 { get; set; }
            public string Empty { get; set; } = "";
            [ExcelColumn(0)]
            public int Col3 { get; set; }
        }


        [Fact]
        public void DataOnSheet2WithRange()
        {
            //Arrange
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("ExcelDestination");

            //Act
            ExcelSource<ExcelDataSheet2> source = new ExcelSource<ExcelDataSheet2>("res/Excel/DataOnSheet2.xlsx")
            {
                Range = new ExcelRange(2, 4, 5, 9),
                SheetName = "Sheet2"
            };

            DBDestination<ExcelDataSheet2> dest = new DBDestination<ExcelDataSheet2>(Connection, "ExcelDestination");

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(5, RowCountTask.Count(Connection, "ExcelDestination"));
            Assert.Equal(1, RowCountTask.Count(Connection, "ExcelDestination","Col2 = 'Wert1' AND Col3 = 5 AND Col4 = 1"));
            Assert.Equal(1, RowCountTask.Count(Connection, "ExcelDestination", "Col2 IS NULL AND Col3 = 0 AND Col4 = 1.2"));
            Assert.Equal(1, RowCountTask.Count(Connection, "ExcelDestination", "Col2 IS NULL AND Col3 = 7 AND Col4 = 1.234"));
            Assert.Equal(1, RowCountTask.Count(Connection, "ExcelDestination", "Col2 = 'Wert4' AND Col3 = 8 AND Col4 = 1.2345"));
            Assert.Equal(1, RowCountTask.Count(Connection, "ExcelDestination", "Col2 = 'Wert5' AND Col3 = 9 AND Col4 = 2"));
        }
    }
}
