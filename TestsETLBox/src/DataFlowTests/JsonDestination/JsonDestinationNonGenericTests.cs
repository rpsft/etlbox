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
    public class JsonDestinationNonGenericTests : IDisposable
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public JsonDestinationNonGenericTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }


        [Fact]
        public void SimpleNonGeneric()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("JsonDestSimpleNonGeneric");
            s2C.InsertTestDataSet3();
            DBSource source = new DBSource(SqlConnection, "JsonDestSimpleNonGeneric");

            //Act
            JsonDestination dest = new JsonDestination("./SimpleNonGeneric.json");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("./SimpleNonGeneric.json"),
                File.ReadAllText("res/JsonDestination/TwoColumnsSet3StringArray.json"));
        }


    }
}
