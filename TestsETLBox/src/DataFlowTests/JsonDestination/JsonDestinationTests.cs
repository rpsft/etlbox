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
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonDestinationTests : IDisposable
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public JsonDestinationTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class MySimpleRow
        {
            public string Col2 { get; set; }
            public int Col1 { get; set; }
        }

        [Fact]
        public void SimpleFlowWithObject()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("JsonDestSimple");
            s2C.InsertTestDataSet3();
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(SqlConnection, "JsonDestSimple");

            //Act
            JsonDestination<MySimpleRow> dest = new JsonDestination<MySimpleRow>("./SimpleWithObject.json");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("res/JsonDestination/TwoColumnsSet3.json")
                , File.ReadAllText("./SimpleWithObject.json"));
        }

        [Fact]
        public void SimpleFlowWithBatchWrite()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("JsonDestBatch");
            s2C.InsertTestDataSet3();
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(SqlConnection, "JsonDestBatch");

            //Act
            JsonDestination<MySimpleRow> dest = new JsonDestination<MySimpleRow>("./ObjectWithBatchWrite.json", 1);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("res/JsonDestination/TwoColumnsSet3.json"),
                File.ReadAllText("./ObjectWithBatchWrite.json"));
        }
    }
}
