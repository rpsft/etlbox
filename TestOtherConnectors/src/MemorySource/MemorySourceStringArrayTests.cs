using ETLBox.Connection;
using ETLBox.DataFlow; using ETLBox.DataFlow.Connectors; using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class MemorySourceStringArrayTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public MemorySourceStringArrayTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void DataIsFromList()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("MemoryDestinationNonGeneric");
            MemorySource<string[]> source = new MemorySource<string[]>();
            DbDestination<string[]> dest = new DbDestination<string[]>(SqlConnection, "MemoryDestinationNonGeneric");

            //Act
            source.DataAsList = new List<string[]>()
            {
                new string[] { "1", "Test1" },
                new string[] { "2", "Test2" },
                new string[] { "3", "Test3" },
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }


    }
}
