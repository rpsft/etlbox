using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.Json;
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
    public class JsonSourceConverterTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public JsonSourceConverterTests(DataFlowDatabaseFixture dbFixture)
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

        [Fact]
        public void JsonPathInEpandoObject()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("JsonSourceNestedDynamic");
            RowTransformation<ExpandoObject> trans = new RowTransformation<ExpandoObject>(
                row =>
                {
                    dynamic r = row as ExpandoObject;
                    r.Col1 = r.Column1;
                    return r;
                });
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(SqlConnection, "JsonSourceNestedDynamic");

            //Act
            JsonSource<ExpandoObject> source = new JsonSource<ExpandoObject>("res/JsonSource/NestedData.json", ResourceType.File);
            List<JsonProperty2JsonPath> pathLookups = new List<JsonProperty2JsonPath>()
            {
                new JsonProperty2JsonPath()
                {
                    JsonPropertyName = "Column2",
                    JsonPath = "Value",
                    NewPropertyName = "Col2"
                 }
            };
            source.JsonSerializer.Converters.Add(new ExpandoJsonPathConverter(pathLookups));

            source.LinkTo(trans).LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }


        [Fact]
        public void JsonPathListIntoDynamic()
        {
            //Arrange
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("JsonSourceNestedDynamic4Cols");
            DbDestination<ExpandoObject> dest = new DbDestination<ExpandoObject>(SqlConnection, "JsonSourceNestedDynamic4Cols");

            //Act
            JsonSource<ExpandoObject> source = new JsonSource<ExpandoObject>("res/JsonSource/NestedData4Cols.json", ResourceType.File);
            List<JsonProperty2JsonPath> pathLookups = new List<JsonProperty2JsonPath>()
            {
                new JsonProperty2JsonPath()
                {
                    JsonPropertyName = "Col2",
                    JsonPath = "Value",
                 },
                new JsonProperty2JsonPath()
                {
                    JsonPropertyName = "Object",
                    JsonPath = "Number[0]",
                    NewPropertyName = "Col4"
                },
                new JsonProperty2JsonPath()
                {
                    JsonPropertyName = "Array",
                    JsonPath = "[1].Value",
                    NewPropertyName = "Col3"
                 }
            };
            source.JsonSerializer.Converters.Add(new ExpandoJsonPathConverter(pathLookups));

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();
            Assert.Equal(1, RowCountTask.Count(SqlConnection, "JsonSourceNestedDynamic4Cols", $"Col2 = 'Test2' AND Col3 = 4711 AND Col4='1.23'"));

        }


    }
}
