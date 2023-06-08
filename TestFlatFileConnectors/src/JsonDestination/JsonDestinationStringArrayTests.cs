using System.IO;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestFlatFileConnectors.Helpers;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.JsonDestination
{
    [Collection("DataFlow")]
    public class JsonDestinationStringArrayTests
    {
        private SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SimpleNonGeneric()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("JsonDestSimpleNonGeneric");
            s2C.InsertTestDataSet3();
            DbSource<string[]> source = new DbSource<string[]>(
                SqlConnection,
                "JsonDestSimpleNonGeneric"
            );

            //Act
            JsonDestination<string[]> dest = new JsonDestination<string[]>(
                "./SimpleNonGeneric.json",
                ResourceType.File
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                File.ReadAllText("res/JsonDestination/TwoColumnsSet3StringArray.json")
                    .NormalizeLineEndings(),
                File.ReadAllText("./SimpleNonGeneric.json")
            );
        }
    }
}
