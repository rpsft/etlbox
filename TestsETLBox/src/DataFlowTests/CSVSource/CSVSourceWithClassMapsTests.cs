using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
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
    public class CSVSourceWithClassMapsTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public CSVSourceWithClassMapsTests(DatabaseFixture dbFixture)
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

        public class ModelClassMap : ClassMap<MySimpleRow>
        {
            public ModelClassMap()
            {
                Map(m => m.Col1).Name("Header1");
                Map(m => m.Col2).Name("Header2");
            }
        }

        [Fact]
        public void SimpleFlowUsingClassMap()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CSVDestination2Columns");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(Connection, "CSVDestination2Columns");

            //Act
            CSVSource<MySimpleRow> source = new CSVSource<MySimpleRow>("res/CSVSource/TwoColumns.csv");
            source.Configuration.RegisterClassMap<ModelClassMap>();
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
