using System.Dynamic;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.XmlSource
{
    [Collection("DataFlow")]
    public class XmlSourceDynamicObjectTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

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
                Connection,
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
