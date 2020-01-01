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
    public class CSVDestinationTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CSVDestinationTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            [Name("Header2")]
            [Index(2)]
            public string Col2 { get; set; }
            [Name("Header1")]
            [Index(1)]
            public int Col1 { get; set; }
        }

        [Fact]
        public void SimpleFlowWithObject()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("CSVDestSimple");
            s2C.InsertTestDataSet3();
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(SqlConnection, "CSVDestSimple");

            //Act
            CSVDestination<MySimpleRow> dest = new CSVDestination<MySimpleRow>("./SimpleWithObject.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("./SimpleWithObject.csv"),
                File.ReadAllText("res/CSVDestination/TwoColumnsSet3.csv"));
        }

        [Fact]
        public void SimpleFlowWithBatchWrite()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture("CSVDestBatch");
            s2C.InsertTestDataSet3();
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(SqlConnection, "CSVDestBatch");

            //Act
            CSVDestination<MySimpleRow> dest = new CSVDestination<MySimpleRow>("./ObjectWithBatchWrite.csv", 2);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("./ObjectWithBatchWrite.csv"),
                File.ReadAllText("res/CSVDestination/TwoColumnsSet3.csv"));
        }
    }
}
