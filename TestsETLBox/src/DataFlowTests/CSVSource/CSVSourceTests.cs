using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
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
    public class CSVSourceTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public CSVSourceTests(DatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class MySimpleRow
        {
            [Name("Header2")]
            public string Col2 { get; set; }
            [Name("Header1")]
            public int Col1 { get; set; }
        }

        [Fact]
        public void SimpleFlowWithObject()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CSVDestination2Columns");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(Connection, "CSVDestination2Columns");

            //Act
            CSVSource<MySimpleRow> source = new CSVSource<MySimpleRow>("res/CSVSource/TwoColumns.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void CSVGenericWithSkipRows_DB()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CSVDestination2Columns");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(Connection, "CSVDestination2Columns");

            //Act
            CSVSource<MySimpleRow> source = new CSVSource<MySimpleRow>("res/CSVSource/TwoColumnsSkipRows.csv");
            source.SkipRows = 2;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
