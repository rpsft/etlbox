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
    public class CSVDestinationNullHandlingTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CSVDestinationNullHandlingTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            [Index(0)]
            public int Col1 { get; set; }
            [Index(1)]
            public string Col2 { get; set; }
        }

        [Fact]
        public void IgnoreWithObject()
        {
            //Arrange
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.Data = new List<MySimpleRow>()
            {
                null,
                new MySimpleRow() { Col1 = 1, Col2 = "Test1"},
                null,
                new MySimpleRow() { Col1 = 2, Col2 = "Test2"},
                new MySimpleRow() { Col1 = 3, Col2 = "Test3"},
                null
            };

            //Act
            CSVDestination<MySimpleRow> dest = new CSVDestination<MySimpleRow>("./IgnoreNullValues.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("./IgnoreNullValues.csv"),
                File.ReadAllText("res/CSVDestination/TwoColumns.csv"));
        }

        [Fact]
        public void IgnoreWithStringArray()
        {
            //Arrange
            MemorySource source = new MemorySource();
            source.Data = new List<string[]>()
            {
                null,
                new string[] { "1", "Test1"},
                null,
                new string[] { "2", "Test2"},
                new string[] { "3", "Test3"},
                null
            };

            //Act
            CSVDestination dest = new CSVDestination("./IgnoreNullValuesStringArray.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("./IgnoreNullValuesStringArray.csv"),
                File.ReadAllText("res/CSVDestination/TwoColumnsNoHeader.csv"));
        }


    }
}
