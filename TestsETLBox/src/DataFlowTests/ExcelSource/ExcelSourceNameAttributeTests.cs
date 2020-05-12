using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class ExcelSourceNameAttributeTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public ExcelSourceNameAttributeTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            [ExcelColumn("Col1")]
            [ColumnMap("Col1")]
            public int Column1 { get; set; }
            [ExcelColumn("Col2")]
            [ColumnMap("Col2")]
            public string Column2 { get; set; }
            public string ExtraColumn { get; set; }
        }

        [Fact]
        public void SimpleData()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("ExcelDestinationWithNameAttribute");
            ExcelSource<MySimpleRow> source = new ExcelSource<MySimpleRow>("res/Excel/TwoColumnWithHeader.xlsx");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(Connection, "ExcelDestinationWithNameAttribute");

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

    }
}
