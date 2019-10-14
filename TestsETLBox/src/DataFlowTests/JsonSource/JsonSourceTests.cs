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
    public class JsonSourceTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public JsonSourceTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void SimpleFlowWithObject()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("JsonSource2Cols");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(Connection, "JsonSource2Cols");

            //Act
            JsonSource<MySimpleRow> source = new JsonSource<MySimpleRow>("res/JsonSource/TwoColumns.json");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

    }
}
