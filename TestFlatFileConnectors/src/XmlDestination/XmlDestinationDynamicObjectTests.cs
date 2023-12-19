using ALE.ETLBox.DataFlow;
using TestFlatFileConnectors.Fixture;
using TestFlatFileConnectors.Helpers;
using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.XmlDestination
{
    public class XmlDestinationDynamicObjectTests : FlatFileConnectorsTestBase
    {
        public XmlDestinationDynamicObjectTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

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
