using System.IO;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using JetBrains.Annotations;
using TestFlatFileConnectors.Helpers;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.JsonDestination
{
    [Collection("DataFlow")]
    public class JsonDestinationTests
    {
        private SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [UsedImplicitly(ImplicitUseTargetFlags.Members)]
        public class MySimpleRow
        {
            public string Col2 { get; set; }
            public int Col1 { get; set; }
        }

        [Fact]
        public void SimpleFlowWithObject()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("JsonDestSimple");
            s2C.InsertTestDataSet3();
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                SqlConnection,
                "JsonDestSimple"
            );

            //Act
            JsonDestination<MySimpleRow> dest = new JsonDestination<MySimpleRow>(
                "./SimpleWithObject.json",
                ResourceType.File
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                File.ReadAllText("res/JsonDestination/TwoColumnsSet3.json").NormalizeLineEndings(),
                File.ReadAllText("./SimpleWithObject.json")
            );
        }
    }
}
