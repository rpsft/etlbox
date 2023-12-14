using ALE.ETLBox.src.Definitions.DataFlow.Type;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestFlatFileConnectors.src;
using TestFlatFileConnectors.src.Fixture;
using TestShared.src.SharedFixtures;

namespace TestFlatFileConnectors.src.XmlSource
{
    public class XmlSourceDynamicObjectTests : FlatFileConnectorsTestBase
    {
        public XmlSourceDynamicObjectTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void SourceWithDifferentNames()
        {
            //Arrange
            var dest2Columns = new TwoColumnsTableFixture(
                "XmlSource2ColsDynamic"
            );
            var trans = new RowTransformation<ExpandoObject>(row =>
            {
                dynamic r = row;
                r.Col1 = r.Column1;
                r.Col2 = r.Column2;
                return r;
            });
            var dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "XmlSource2ColsDynamic"
            );

            //Act
            var source = new XmlSource<ExpandoObject>(
                "res/XmlSource/TwoColumnsElementDifferentNames.xml",
                ResourceType.File
            )
            {
                ElementName = "MySimpleRow"
            };
            source.LinkTo(trans).LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
