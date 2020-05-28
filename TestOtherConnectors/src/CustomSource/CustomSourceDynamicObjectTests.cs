using ETLBox.ConnectionManager;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBox.SqlServer;
using ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using System.Dynamic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CustomSourceDynamicObjectTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CustomSourceDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SimpleFlow()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("Destination4CustomSourceDynamic");
            List<string> Data = new List<string>() { "Test1", "Test2", "Test3" };
            int _readIndex = 0;
            Func<ExpandoObject> ReadData = () =>
            {
                dynamic result = new ExpandoObject();
                result.Col1 = (_readIndex + 1).ToString();
                result.Col2 = Data[_readIndex];
                _readIndex++;
                return result;
            };

            Func<bool> EndOfData = () => _readIndex >= Data.Count;

            //Act
            CustomSource<ExpandoObject> source = new CustomSource<ExpandoObject>(ReadData, EndOfData);
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(Connection, "Destination4CustomSourceDynamic");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
