using ETLBox.Connection;
using ETLBox.DataFlow;
using ETLBox.DataFlow;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Dynamic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonSourceDynamicObjectTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public JsonSourceDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SourceWithDifferentNames()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("JsonSource2ColsDynamic");
            RowTransformation<ExpandoObject> trans = new RowTransformation<ExpandoObject>(
                row =>
                {
                    dynamic r = row as ExpandoObject;
                    r.Col1 = r.Column1;
                    r.Col2 = r.Column2;
                    return r;
                });
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(Connection, "JsonSource2ColsDynamic");

            //Act
            JsonSource<ExpandoObject> source = new JsonSource<ExpandoObject>("res/JsonSource/TwoColumnsDifferentNames.json", ResourceType.File);
            source.LinkTo(trans).LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void JsonWithDecimalType()
        {
            //Arrange
            FourColumnsTableFixture d4c = new FourColumnsTableFixture("JsonSource4ColsDynamic");
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(Connection, "JsonSource4ColsDynamic");

            //Act
            JsonSource<ExpandoObject> source = new JsonSource<ExpandoObject>("res/JsonSource/FourColumns.json", ResourceType.File);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d4c.AssertTestData();
        }
    }
}
