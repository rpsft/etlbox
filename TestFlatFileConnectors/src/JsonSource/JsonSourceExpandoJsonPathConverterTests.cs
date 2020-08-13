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
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonSourceExpandoJsonPathConverterTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public JsonSourceExpandoJsonPathConverterTests(DataFlowDatabaseFixture dbFixture)
        {
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
                    SearchPropertyName = "Column2",
                    JsonPath = "$.Value",
                    OutputPropertyName = "Col2"
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
        public void WithMultipleTokens()
        {
            //Arrange
            MemoryDestination dest = new MemoryDestination();

            //Act
            JsonSource<ExpandoObject> source = new JsonSource<ExpandoObject>("res/JsonSource/NestedData.json", ResourceType.File);
            List<JsonProperty2JsonPath> pathLookups = new List<JsonProperty2JsonPath>()
            {
                new JsonProperty2JsonPath()
                {
                    SearchPropertyName = "Column2",
                    JsonPath = "$.Value",
                    OutputPropertyName = "Value"
                 },
                new JsonProperty2JsonPath() {
                    SearchPropertyName = "Column2",
                    JsonPath = "$['Id']",
                    OutputPropertyName = "Id"
                 }
            };
            source.JsonSerializer.Converters.Add(new ExpandoJsonPathConverter(pathLookups));

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection<ExpandoObject>(dest.Data,
                row => { dynamic r = row as ExpandoObject; Assert.True(r.Column1 == 1 && r.Id == "A" && r.Value == "Test1"); },
                row => { dynamic r = row as ExpandoObject; Assert.True(r.Column1 == 2 && r.Id == "B" && r.Value == "Test2"); },
                row => { dynamic r = row as ExpandoObject; Assert.True(r.Column1 == 3 && r.Id == "C" && r.Value == "Test3"); }
                );
        }

        [Fact]
        public void NestDataWithNullValues()
        {
            //Arrange
            MemoryDestination dest = new MemoryDestination();

            //Act
            JsonSource<ExpandoObject> source = new JsonSource<ExpandoObject>("res/JsonSource/NestedDataNullValues.json", ResourceType.File);
            List<JsonProperty2JsonPath> pathLookups = new List<JsonProperty2JsonPath>()
            {
                new JsonProperty2JsonPath()
                {
                    SearchPropertyName = "Column2",
                    JsonPath = "$.Value",
                    OutputPropertyName = "Value"
                 },
                new JsonProperty2JsonPath() {
                    SearchPropertyName = "Column2",
                    JsonPath = "$['Id']",
                    OutputPropertyName = "Id"
                 }
            };
            source.JsonSerializer.Converters.Add(new ExpandoJsonPathConverter(pathLookups));

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection<ExpandoObject>(dest.Data,
                row => { dynamic r = row as ExpandoObject; Assert.True(r.Column1 == 1 && r.Id == "A" && r.Value == "Test1"); },
                row => { dynamic r = row as ExpandoObject; Assert.True(r.Column1 == 2 && r.Id == "B" && r.Value == null); },
                row => {
                    dynamic r = row as ExpandoObject;
                    Assert.True(r.Column1 == 3 && r.Id == "C");
                    Assert.False(((IDictionary<string, object>)r).ContainsKey("Value"));
                }
                );
        }


        [Fact]
        public void DifferentDataTypes()
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
                    SearchPropertyName = "Col2",
                    JsonPath = "Value",
                 },
                new JsonProperty2JsonPath()
                {
                    SearchPropertyName = "Object",
                    JsonPath = "Number[0]",
                    OutputPropertyName = "Col4"
                },
                new JsonProperty2JsonPath()
                {
                    SearchPropertyName = "Array",
                    JsonPath = "[1].Value",
                    OutputPropertyName = "Col3"
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


        [Fact]
        public void WithNestedArraysInside()
        {
            //Arrange
            MemoryDestination dest = new MemoryDestination();
            RowTransformation<ExpandoObject> trans = new RowTransformation<ExpandoObject>(
                row =>
                {
                    dynamic r = row as ExpandoObject;
                    return r;
                });

            //Act
            JsonSource<ExpandoObject> source = new JsonSource<ExpandoObject>("res/JsonSource/NestedArray.json", ResourceType.File);
            List<JsonProperty2JsonPath> pathLookups = new List<JsonProperty2JsonPath>()
            {
                new JsonProperty2JsonPath()
                {
                    SearchPropertyName = "Column2",
                    JsonPath = "$.[*].ArrayCol1",
                    OutputPropertyName = "ArrayCol1"
                 },
                new JsonProperty2JsonPath()
                {
                    SearchPropertyName = "Column2",
                    JsonPath = "$.[*].ArrayCol2",
                    OutputPropertyName = "ArrayCol2"
                 }
            };
            source.JsonSerializer.Converters.Add(new ExpandoJsonPathConverter(pathLookups));

            source.LinkTo(trans).LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection<ExpandoObject>(dest.Data,
                row => {
                    dynamic r = row as ExpandoObject; var lac1 = r.ArrayCol1 as List<object>; var lac2 = r.ArrayCol2;
                    Assert.True(r.Column1 == 1 && lac1.Count == 2 && lac2.Count == 2 );
                },
                 row => {
                     dynamic r = row as ExpandoObject; var lac1 = r.ArrayCol1 as List<object>; var lac2 = r.ArrayCol2;
                     Assert.True(r.Column1 == 2 && lac1.Count == 2 && lac2.Count == 2);
                 },
                 row => {
                     dynamic r = row as ExpandoObject; Assert.True(r.Column1 == 3 && r.ArrayCol1 == "E" && r.ArrayCol2 == "TestE");
                 }
            );
        }

        [Fact]
        public void WithNestedArraysAsJsonString()
        {
            //Arrange
            MemoryDestination dest = new MemoryDestination();
            RowTransformation<ExpandoObject> trans = new RowTransformation<ExpandoObject>(
                row =>
                {
                    dynamic r = row as ExpandoObject;
                    return r;
                });

            //Act
            JsonSource<ExpandoObject> source = new JsonSource<ExpandoObject>("res/JsonSource/NestedArray.json", ResourceType.File);
            List<JsonProperty2JsonPath> pathLookups = new List<JsonProperty2JsonPath>()
            {
                new JsonProperty2JsonPath()
                {
                    SearchPropertyName = "Column2",
                    JsonPath = "$.[*]",
                    OutputPropertyName = "ArrayCol1"
                 }
            };
            source.JsonSerializer.Converters.Add(new ExpandoJsonPathConverter(pathLookups));

            source.LinkTo(trans).LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection<ExpandoObject>(dest.Data,
                row => {
                    dynamic r = row as ExpandoObject; var lac1 = r.ArrayCol1 as List<object>;
                    Assert.True(r.Column1 == 1 && lac1.Count == 2 &&
                        RemoveWhiteSpace(lac1.First().ToString()) == @"{""ArrayCol1"":""A"",""ArrayCol2"":""TestA""}");
                },
                 row => {
                     dynamic r = row as ExpandoObject; var lac1 = r.ArrayCol1 as List<object>;
                     Assert.True(r.Column1 == 2 && lac1.Count == 2);
                 },
                 row => {
                     dynamic r = row as ExpandoObject; Assert.True(r.Column1 == 3 && RemoveWhiteSpace(r.ArrayCol1) == @"{""ArrayCol1"":""E"",""ArrayCol2"":""TestE""}");
                 }
            );
        }

        [Fact]
        public void ReQueryNestedArrays()
        {
            //Arrange
            MemoryDestination dest = new MemoryDestination();
            RowTransformation<ExpandoObject> trans = new RowTransformation<ExpandoObject>(
                row =>
                {
                    dynamic r = row as ExpandoObject;
                    return r;
                });

            //Act
            JsonSource<ExpandoObject> source = new JsonSource<ExpandoObject>("res/JsonSource/ReQueryNestedArray.json", ResourceType.File);
            List<JsonProperty2JsonPath> pathLookups = new List<JsonProperty2JsonPath>()
            {
                new JsonProperty2JsonPath()
                {
                    JsonPath = "$.[*]",
                    SearchPropertyName = "arraycolumn",
                    ReQueryJsonPathOutput = new List<JsonProperty2JsonPath>() {
                        new JsonProperty2JsonPath() {
                            JsonPath = "$.arrayvalue1",
                            OutputPropertyName = "arrayvalue1"
                        },
                        new JsonProperty2JsonPath() {
                            JsonPath = "$.arrayvalue2",
                            OutputPropertyName = "arrayvalue2"
                        }
                    }
                }
            };
            source.JsonSerializer.Converters.Add(new ExpandoJsonPathConverter(pathLookups));

            source.LinkTo(trans).LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Collection<ExpandoObject>(dest.Data,
                row => {
                    dynamic r = row as ExpandoObject;
                    Assert.True(r.idcolumn1 == 1);
                    Assert.True(r.arraycolumn.Count == 2);
                    Assert.True(r.arraycolumn[0].arrayvalue1 == "Test1_1");
                    Assert.True(r.arraycolumn[0].arrayvalue2 == "A");
                    Assert.True(r.arraycolumn[1].arrayvalue1 == "Test1_2");
                    Assert.True(r.arraycolumn[1].arrayvalue2 == null);
                },
                 row => {
                     dynamic r = row as ExpandoObject;
                     Assert.True(r.idcolumn1 == 2);
                     Assert.True(r.arraycolumn.Count == 1);
                     Assert.True(r.arraycolumn[0].arrayvalue1 == "Test2_1");
                     Assert.True(r.arraycolumn[0].arrayvalue2 == "B");
                 },
                 row => {
                     dynamic r = row as ExpandoObject;
                     Assert.True(r.idcolumn1 == 3);
                     Assert.True(r.arraycolumn.Count == 3);
                     Assert.True(r.arraycolumn[0].arrayvalue1 == "Test3_1");
                     Assert.True(r.arraycolumn[0].arrayvalue2 == "C");
                     Assert.True(r.arraycolumn[1].arrayvalue1 == "Test3_2");
                     Assert.True(r.arraycolumn[1].arrayvalue2 == null);
                     Assert.True(r.arraycolumn[2].arrayvalue1 == "Test3_3");
                     Assert.True(r.arraycolumn[2].arrayvalue2 == "D");
                 }
            );
        }

        public static string RemoveWhiteSpace(string sql)
        {
            return Regex.Replace(sql, @"\s+", "");
        }
    }
}
