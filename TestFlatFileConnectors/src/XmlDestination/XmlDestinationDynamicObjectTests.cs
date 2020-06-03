using ETLBox.Connection;
using ETLBox.DataFlow;
using ETLBox.DataFlow;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Dynamic;
using System.IO;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class XmlDestinationDynamicObjectTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public XmlDestinationDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SimpleFlowWithObject()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("XmlDestDynamic");
            s2C.InsertTestDataSet3();
            DbSource<ExpandoObject> source = new DbSource<ExpandoObject>(SqlConnection, "XmlDestDynamic");

            //Act
            XmlDestination<ExpandoObject> dest = new XmlDestination<ExpandoObject>("./SimpleWithDynamicObject.xml", ResourceType.File);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("res/XmlDestination/TwoColumnsSet3DynamicObject.xml"),
                File.ReadAllText("./SimpleWithDynamicObject.xml"));
        }
    }
}
