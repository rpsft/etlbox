using System.Dynamic;
using System.IO;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using TestFlatFileConnectors.Helpers;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class XmlDestinationDynamicObjectTests
    {
        public XmlDestinationDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        private SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SimpleFlowWithObject()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture("XmlDestDynamic");
            s2C.InsertTestDataSet3();
            var source = new DbSource<ExpandoObject>(SqlConnection, "XmlDestDynamic");

            //Act
            var dest = new XmlDestination<ExpandoObject>("./SimpleWithDynamicObject.xml", ResourceType.File);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("res/XmlDestination/TwoColumnsSet3DynamicObject.xml").NormalizeLineEndings(),
                File.ReadAllText("./SimpleWithDynamicObject.xml"));
        }
    }
}