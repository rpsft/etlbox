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
    public class CSVSourceNoHeaderTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CSVSourceNoHeaderTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            [Index(1)]
            public string Col2 { get; set; }
            [Index(0)]
            public int Col1 { get; set; }
        }

        [Fact]
        public void CSVSourceNoHeader()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CSVSourceNoHeader");
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(Connection, "CSVSourceNoHeader");

            //Act
            CsvSource<MySimpleRow> source = new CsvSource<MySimpleRow>("res/CSVSource/TwoColumnsNoHeader.csv");
            source.Configuration.HasHeaderRecord = false;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
