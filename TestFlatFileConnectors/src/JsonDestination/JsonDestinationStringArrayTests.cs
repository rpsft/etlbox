using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using System.IO;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonDestinationStringArrayTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public JsonDestinationStringArrayTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SimpleNonGeneric()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("JsonDestSimpleNonGeneric");
            s2C.InsertTestDataSet3();
            DbSource<string[]> source = new DbSource<string[]>(SqlConnection, "JsonDestSimpleNonGeneric");

            //Act
            JsonDestination<string[]> dest = new JsonDestination<string[]>("./SimpleNonGeneric.json", ResourceType.File);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("res/JsonDestination/TwoColumnsSet3StringArray.json")
                , File.ReadAllText("./SimpleNonGeneric.json"));
        }


    }
}
