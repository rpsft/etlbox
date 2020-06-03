using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBox.SqlServer;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Linq;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class BlockTransformationTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public BlockTransformationTests(DataFlowDatabaseFixture dbFixture)
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

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(Connection, "BlockTransSource");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(Connection, "BlockTransDest");

            //Act
            BlockTransformation<MySimpleRow> block = new BlockTransformation<MySimpleRow>(
                inputData =>
                {
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

            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(Connection, "BlockTransSource");
            DbDestination<MyOtherRow> dest = new DbDestination<MyOtherRow>(Connection, "BlockTransDest");

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
