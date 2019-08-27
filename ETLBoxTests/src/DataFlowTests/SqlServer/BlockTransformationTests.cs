using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests.SqlServer
{
    [Collection("Sql Server DataFlow")]
    public class BlockTransformationTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public BlockTransformationTests(DatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void ModifyInputDataList()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("BlockTransSource");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("BlockTransDest");

            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(Connection, "BlockTransSource");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(Connection, "BlockTransDest");

            //Act
            BlockTransformation<MySimpleRow> block = new BlockTransformation<MySimpleRow>(
                inputData => {
                    inputData.RemoveRange(1, 2);
                    inputData.Add(new MySimpleRow() { Col1 = 4, Col2 = "Test4" });
                    return inputData;
                });
            source.LinkTo(block);
            block.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(2, RowCountTask.Count(Connection, "BlockTransDest"));
            Assert.Equal(1, RowCountTask.Count(Connection, "BlockTransDest", "Col1 = 1 AND Col2='Test1'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "BlockTransDest", "Col1 = 4 AND Col2='Test4'"));
        }

        public class MyOtherRow
        {
            [ColumnMap("Col1")]
            public int Col3 { get; set; }
            [ColumnMap("Col2")]
            public string Col4 { get; set; }
        }

        [Fact]
        public void ConvertObjects()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("BlockTransSource");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("BlockTransDest");

            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(Connection , "BlockTransSource");
            DBDestination<MyOtherRow> dest = new DBDestination<MyOtherRow>(Connection, "BlockTransDest");

            //Act
            BlockTransformation<MySimpleRow, MyOtherRow> block = new BlockTransformation<MySimpleRow, MyOtherRow>(
                inputData =>
                {
                    return inputData.Select(row => new MyOtherRow() { Col3 = row.Col1, Col4 = row.Col2 }).ToList();
                });
            source.LinkTo(block);
            block.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
