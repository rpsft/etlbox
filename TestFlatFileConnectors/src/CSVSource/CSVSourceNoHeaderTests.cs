using CsvHelper.Configuration.Attributes;
using ETLBox.Connection;
using ETLBox.DataFlow.Connectors;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CsvSourceNoHeaderTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CsvSourceNoHeaderTests(DataFlowDatabaseFixture dbFixture)
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
        public void CsvSourceNoHeader()
        {
            //Arrange
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CsvSourceNoHeader");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(Connection, "CsvSourceNoHeader");

            //Act
            CsvSource<MySimpleRow> source = new CsvSource<MySimpleRow>("res/CsvSource/TwoColumnsNoHeader.csv");
            source.Configuration.HasHeaderRecord = false;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }
    }
}
