using ETLBox.ConnectionManager;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CustomSourceStringArrayTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CustomSourceStringArrayTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SimpleFlow()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("Destination4CustomSourceNonGeneric");
            List<string> Data = new List<string>() { "Test1", "Test2", "Test3" };
            int _readIndex = 0;
            Func<string[]> ReadData = () =>
            {
                string[] result = new string[2];
                result[0] = (_readIndex + 1).ToString();
                result[1] = Data[_readIndex];
                _readIndex++;
                return result;
            };

            Func<bool> EndOfData = () => _readIndex >= Data.Count;

            //Act
            CustomSource<string[]> source = new CustomSource<string[]>(ReadData, EndOfData);
            DbDestination<string[]> dest = new DbDestination<string[]>(Connection, "Destination4CustomSourceNonGeneric");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
