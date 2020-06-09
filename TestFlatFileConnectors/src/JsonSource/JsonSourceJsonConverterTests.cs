using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBox.Helper;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System.Collections.Generic;
using System.Dynamic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonSourceJsonConverterTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public JsonSourceJsonConverterTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [JsonConverter(typeof(JsonPathConverter))]
        public class MySimpleRow
        {
            [JsonProperty("Column1")]
            public int Col1 { get; set; }
            [JsonProperty("Column2.Value")]
            public string Col2 { get; set; }
        }

        [Fact]
        public void JsonPathInJsonPropertyAttribute()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("JsonSourceNested");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(SqlConnection, "JsonSourceNested");

            //Act
            JsonSource<MySimpleRow> source = new JsonSource<MySimpleRow>("res/JsonSource/NestedData.json", ResourceType.File);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

    }
}
