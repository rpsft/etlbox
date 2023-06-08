using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.JsonSource
{
    [Collection("DataFlow")]
    public class JsonSourceStringArrayTests
    {
        private SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SimpleFlowWithStringArray()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "JsonSource2ColsNonGen"
            );
            DbDestination<string[]> dest = new DbDestination<string[]>(
                Connection,
                "JsonSource2ColsNonGen"
            );

            //Act
            JsonSource<string[]> source = new JsonSource<string[]>(
                "res/JsonSource/TwoColumnsStringArray.json",
                ResourceType.File
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
