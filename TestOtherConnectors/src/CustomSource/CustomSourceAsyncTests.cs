using ETLBox;
using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CustomSourceAsyncTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CustomSourceAsyncTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void SimpleAsyncFlow()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("Destination4CustomSource");
            List<string> Data = new List<string>() { "Test1", "Test2", "Test3" };
            int _readIndex = 0;
            Func<MySimpleRow> ReadData = () =>
            {
                if (_readIndex == 0) Task.Delay(300).Wait();
                var result = new MySimpleRow()
                {
                    Col1 = _readIndex + 1,
                    Col2 = Data[_readIndex]
                };
                _readIndex++;
                return result;
            };

            Func<bool> EndOfData = () => _readIndex >= Data.Count;

            //Act
            CustomSource<MySimpleRow> source = new CustomSource<MySimpleRow>(ReadData, EndOfData);
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(SqlConnection, "Destination4CustomSource");
            source.LinkTo(dest);
            Task sourceT = source.ExecuteAsync();
            Task destT = dest.Completion;

            //Assert
            Assert.True(RowCountTask.Count(SqlConnection, "Destination4CustomSource") == 0);
            sourceT.Wait();
            destT.Wait();
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void ExceptionalAsyncFlow()
        {
            //Arrange
            CustomSource source = new CustomSource(
                () => throw new ETLBoxException("Test Exception"), () => false
            );
            CustomDestination dest = new CustomDestination
                (row => {; });

            //Act
            source.LinkTo(dest);

            //Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                Task sourceT = source.ExecuteAsync();
                Task destT = dest.Completion;
                try
                {
                    sourceT.Wait();
                    destT.Wait();
                }
                catch (Exception e)
                {
                    throw e.InnerException;
                }
            });

        }
    }
}
