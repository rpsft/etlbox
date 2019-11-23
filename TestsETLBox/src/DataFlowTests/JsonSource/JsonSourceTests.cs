using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using CsvHelper.Configuration.Attributes;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonSourceTests
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public JsonSourceTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void JsonFromFile()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("JsonSource2Cols");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(Connection, "JsonSource2Cols");

            //Act
            JsonSource<MySimpleRow> source = new JsonSource<MySimpleRow>("res/JsonSource/TwoColumns.json", ResourceType.File);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        public class Todo
        {
            [JsonProperty("Id")]
            public int Key { get; set; }
            public string Title { get; set; }
        }

        [Fact]
        public void JsonFromWebService()
        {
            //Arrange
            MemoryDestination<Todo> dest = new MemoryDestination<Todo>(200);

            //Act
            JsonSource<Todo> source = new JsonSource<Todo>("https://jsonplaceholder.typicode.com/todos");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.All(dest.Data, item => Assert.True(item.Key > 0));
        }

    }
}
