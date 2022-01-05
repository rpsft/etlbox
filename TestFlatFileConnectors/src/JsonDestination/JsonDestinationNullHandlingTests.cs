using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using TestFlatFileConnectors.Helpers;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonDestinationNullHandlingTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public JsonDestinationNullHandlingTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void IgnoreWithObject()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList = new List<MySimpleRow>()
            {
                null,
                new MySimpleRow() { Col1 = 1, Col2 = "Test1"},
                null,
                new MySimpleRow() { Col1 = 2, Col2 = "Test2"},
                new MySimpleRow() { Col1 = 3, Col2 = "Test3"},
                null
            };

            //Act
            JsonDestination<MySimpleRow> dest = new JsonDestination<MySimpleRow>("./IgnoreNullValues.json", ResourceType.File);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("./IgnoreNullValues.json"),
                File.ReadAllText("res/JsonDestination/TwoColumns.json").NormalizeLineEndings());
        }

        [Fact]
        public void IgnoreWithStringArray()
        {
            //Arrange
            MemorySource<string[]> source = new MemorySource<string[]>();
            source.DataAsList = new List<string[]>()
            {
                null,
                new string[] { "1", "Test1"},
                null,
                new string[] { "2", "Test2"},
                new string[] { "3", "Test3"},
                null
            };

            //Act
            JsonDestination<string[]> dest = new JsonDestination<string[]>("./IgnoreNullValuesStringArray.json", ResourceType.File);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("./IgnoreNullValuesStringArray.json"),
                File.ReadAllText("res/JsonDestination/TwoColumnsStringArray.json").NormalizeLineEndings());
        }


    }
}
