using CsvHelper.Configuration.Attributes;
using ETLBox.Connection;
using ETLBox.DataFlow.Connectors;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using System.IO;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvDestinationTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CsvDestinationTests(DataFlowDatabaseFixture dbFixture)
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
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(SqlConnection, "CSVDestSimple");

            //Act
            CsvDestination<MySimpleRow> dest = new CsvDestination<MySimpleRow>("./SimpleWithObject.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(File.ReadAllText("./SimpleWithObject.csv"),
                File.ReadAllText("res/CsvDestination/TwoColumnsSet3.csv"));
        }
    }
}
