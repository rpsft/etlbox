using System.Dynamic;
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
    public class JsonDestinationDynamicObjectTests
    {
        private SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SimpleFlowWithObject()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture("JsonDestDynamic");
            s2C.InsertTestDataSet3();
            var source = new DbSource<ExpandoObject>(SqlConnection, "JsonDestDynamic");

            //Act
            var dest = new JsonDestination<ExpandoObject>(
                "./SimpleWithDynamicObject.json",
                ResourceType.File
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            //Null values can't be ignored:
            //https://github.com/JamesNK/Newtonsoft.Json/issues/1466
            Assert.Equal(
                File.ReadAllText("res/JsonDestination/TwoColumnsSet3DynamicObject.json")
                    .NormalizeLineEndings(),
                File.ReadAllText("./SimpleWithDynamicObject.json")
            );
        }
    }
}
