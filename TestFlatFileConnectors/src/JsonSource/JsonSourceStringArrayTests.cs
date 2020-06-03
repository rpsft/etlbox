using ETLBox.Connection;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonSourceStringArrayTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public JsonSourceStringArrayTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SimpleFlowWithStringArray()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("JsonSource2ColsNonGen");
            DbDestination<string[]> dest = new DbDestination<string[]>(Connection, "JsonSource2ColsNonGen");

            //Act
            JsonSource<string[]> source = new JsonSource<string[]>("res/JsonSource/TwoColumnsStringArray.json", ResourceType.File);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
