using System;
using System.Collections.Generic;
using System.Dynamic;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestOtherConnectors.CustomSource
{
    [Collection("DataFlow")]
    public class CustomSourceDynamicObjectTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SimpleFlow()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "Destination4CustomSourceDynamic"
            );
            List<string> Data = new List<string> { "Test1", "Test2", "Test3" };
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
            CustomSource<ExpandoObject> source = new CustomSource<ExpandoObject>(
                ReadData,
                EndOfData
            );
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(
                Connection,
                "Destination4CustomSourceDynamic"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
