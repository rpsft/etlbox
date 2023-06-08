using System;
using System.Collections.Generic;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestOtherConnectors.CustomSource
{
    [Collection("DataFlow")]
    public class CustomSourceErrorLinkingTests
    {
        public SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void SimpleFlow()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "ErrorLinkingCustomSource"
            );
            List<string> Data = new List<string> { "Test1", "Test2", "Test3", "Test4" };
            int _readIndex = 0;
            Func<MySimpleRow> ReadData = () =>
            {
                var result = new MySimpleRow { Col1 = _readIndex + 1, Col2 = Data[_readIndex] };
                _readIndex++;
                if (_readIndex == 4)
                    throw new Exception("Error record!");
                return result;
            };

            Func<bool> EndOfData = () => _readIndex >= Data.Count;

            CustomSource<MySimpleRow> source = new CustomSource<MySimpleRow>(ReadData, EndOfData);
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                SqlConnection,
                "ErrorLinkingCustomSource"
            );
            MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();

            //Act
            source.LinkTo(dest);
            source.LinkErrorTo(errorDest);
            source.Execute();
            dest.Wait();
            errorDest.Wait();

            //Assert
            dest2Columns.AssertTestData();
            Assert.Collection(
                errorDest.Data,
                d =>
                    Assert.True(
                        !string.IsNullOrEmpty(d.RecordAsJson) && !string.IsNullOrEmpty(d.ErrorText)
                    )
            );
        }
    }
}
