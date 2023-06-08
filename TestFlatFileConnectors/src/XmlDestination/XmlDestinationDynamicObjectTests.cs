using System.Dynamic;
using System.IO;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestFlatFileConnectors.Helpers;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.XmlDestination
{
    [Collection("DataFlow")]
    public class XmlDestinationDynamicObjectTests
    {
        private SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SimpleFlowWithObject()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture("XmlDestDynamic");
            s2C.InsertTestDataSet3();
            var source = new DbSource<ExpandoObject>(SqlConnection, "XmlDestDynamic");

            //Act
            var dest = new XmlDestination<ExpandoObject>(
                "./SimpleWithDynamicObject.xml",
                ResourceType.File
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                File.ReadAllText("res/XmlDestination/TwoColumnsSet3DynamicObject.xml")
                    .NormalizeLineEndings(),
                File.ReadAllText("./SimpleWithDynamicObject.xml")
            );
        }
    }
}
