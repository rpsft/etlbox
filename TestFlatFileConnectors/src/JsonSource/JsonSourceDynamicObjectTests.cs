using System.Dynamic;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;
using Xunit;

namespace TestFlatFileConnectors.JsonSource
{
    [Collection("DataFlow")]
    public class JsonSourceDynamicObjectTests
    {
        public SqlConnectionManager Connection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        [Fact]
        public void SourceWithDifferentNames()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                "JsonSource2ColsDynamic"
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
                "JsonSource2ColsDynamic"
            );

            //Act
            JsonSource<ExpandoObject> source = new JsonSource<ExpandoObject>(
                "res/JsonSource/TwoColumnsDifferentNames.json",
                ResourceType.File
            );
            source.LinkTo(trans).LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
