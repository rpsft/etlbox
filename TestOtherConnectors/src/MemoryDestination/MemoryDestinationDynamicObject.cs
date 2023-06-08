using System.Dynamic;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestOtherConnectors.MemoryDestination
{
    [Collection("DataFlow")]
    public class MemoryDestinationDynamicObjectTests
    {
        private SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void DataIsInList()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                "MemoryDestinationSource"
            );
            source2Columns.InsertTestData();

            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>(
                SqlConnection,
                "MemoryDestinationSource"
            );
            MemoryDestination<ExpandoObject> dest = new MemoryDestination<ExpandoObject>();

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            int index = 1;
            foreach (dynamic d in dest.Data)
            {
                Assert.True(d.Col1 == index && d.Col2 == "Test" + index);
                index++;
            }
        }
    }
}
