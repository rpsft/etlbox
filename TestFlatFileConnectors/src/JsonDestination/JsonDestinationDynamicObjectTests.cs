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
using System.Dynamic;
using System.IO;
using System.Linq;
using TestFlatFileConnectors.Helpers;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class JsonDestinationDynamicObjectTests
    {
        private SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public JsonDestinationDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SimpleFlowWithObject()
        {
            //Arrange
            var s2C = new TwoColumnsTableFixture("JsonDestDynamic");
            s2C.InsertTestDataSet3();
            var source = new DbSource<ExpandoObject>(SqlConnection, "JsonDestDynamic");

            //Act
            var dest = new JsonDestination<ExpandoObject>("./SimpleWithDynamicObject.json", ResourceType.File);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            //Null values can't be ignored:
            //https://github.com/JamesNK/Newtonsoft.Json/issues/1466
            Assert.Equal(File.ReadAllText("res/JsonDestination/TwoColumnsSet3DynamicObject.json").NormalizeLineEndings(),
                File.ReadAllText("./SimpleWithDynamicObject.json"));
        }
    }
}
