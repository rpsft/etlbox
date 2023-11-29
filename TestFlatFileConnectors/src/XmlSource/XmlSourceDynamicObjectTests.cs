using ALE.ETLBox.Common.DataFlow;
using TestShared.SharedFixtures;

namespace TestFlatFileConnectors.XmlSource
{
    public class XmlSourceDynamicObjectTests : FlatFileConnectorsTestBase
    {
        public XmlSourceDynamicObjectTests(FlatFileToDatabaseFixture fixture)
            : base(fixture) { }

        [Fact]
        public void SourceWithDifferentNames()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "XmlSource2ColsDynamic"
            );
            RowTransformation<ExpandoObject> trans = new RowTransformation<ExpandoObject>(row =>
            {
                dynamic r = row;
                r.Col1 = r.Column1;
                r.Col2 = r.Column2;
                return r;
            });
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(
                SqlConnection,
                "XmlSource2ColsDynamic"
            );

            //Act
            XmlSource<ExpandoObject> source = new XmlSource<ExpandoObject>(
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
