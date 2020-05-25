using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class BlockTransformationExceptionTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public BlockTransformationExceptionTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void ThrowExceptionWithoutHandling()
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
                    throw new Exception("Test");
                });
            source.LinkTo(block);
            block.LinkTo(dest);

            //Assert
            Assert.Throws<AggregateException>(() =>
            {
                source.Execute();
                dest.Wait();
            });
        }
    }
}
