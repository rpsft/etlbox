using ETLBox.ConnectionManager;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBox.SqlServer;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbDestinationNullHandlingTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public DbDestinationNullHandlingTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void IgnoreWithObject()
        {
            //Arrange
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(SqlConnection, "DestIgnoreNullValues");
            MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
            source.DataAsList = new List<MySimpleRow>()
            {
                null,
                new MySimpleRow() { Col1 = 1, Col2 = "Test1"},
                null,
                new MySimpleRow() { Col1 = 2, Col2 = "Test2"},
                new MySimpleRow() { Col1 = 3, Col2 = "Test3"},
                null
            };
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(SqlConnection, "DestIgnoreNullValues");


            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }

        [Fact]
        public void IgnoreWithStringArray()
        {
            //Arrange
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(SqlConnection, "DestIgnoreNullValuesStringArray");
            MemorySource<string[]> source = new MemorySource<string[]>();
            source.DataAsList = new List<string[]>()
            {
                null ,
                new string[] { "1", "Test1"},
                null,
                new string[] { "2", "Test2"},
                new string[] { "3", "Test3"},
                null
            };
            DbDestination<string[]> dest = new DbDestination<string[]>(SqlConnection, "DestIgnoreNullValuesStringArray");


            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }
    }
}
