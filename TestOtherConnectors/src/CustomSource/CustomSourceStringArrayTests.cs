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
    public class CustomSourceStringArrayTests
    {
        private SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SimpleFlow()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "Destination4CustomSourceNonGeneric"
            );
            List<string> Data = new List<string> { "Test1", "Test2", "Test3" };
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
            DbDestination<string[]> dest = new DbDestination<string[]>(
                Connection,
                "Destination4CustomSourceNonGeneric"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
