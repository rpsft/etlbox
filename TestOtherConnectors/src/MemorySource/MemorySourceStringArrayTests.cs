using System.Collections.Generic;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestOtherConnectors.MemorySource
{
    [Collection("DataFlow")]
    public class MemorySourceStringArrayTests
    {
        public SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void DataIsFromList()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "MemoryDestinationNonGeneric"
            );
            MemorySource<string[]> source = new MemorySource<string[]>();
            DbDestination<string[]> dest = new DbDestination<string[]>(
                SqlConnection,
                "MemoryDestinationNonGeneric"
            );

            //Act
            source.DataAsList = new List<string[]>
            {
                new[] { "1", "Test1" },
                new[] { "2", "Test2" },
                new[] { "3", "Test3" },
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
