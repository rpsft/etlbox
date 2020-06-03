using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow.Connectors;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class CustomDestinationTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");
        public CustomDestinationTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MySimpleRow
        {
            public int Col1 { get; set; }
            public string Col2 { get; set; }
        }

        [Fact]
        public void InsertIntoTable()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("Source");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CustomDestination");

            //Act
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(SqlConnection, "Source");
            CustomDestination<MySimpleRow> dest = new CustomDestination<MySimpleRow>(
                row =>
                {
                    SqlTask.ExecuteNonQuery(SqlConnection, "Insert row",
                        $"INSERT INTO dbo.CustomDestination VALUES({row.Col1},'{row.Col2}')");
                }
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [Fact]
        public void CreateJsonFile()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("JSonSource");
            source2Columns.InsertTestData();
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(SqlConnection, "JSonSource");
            List<MySimpleRow> rows = new List<MySimpleRow>();

            //Act
            CustomDestination<MySimpleRow> dest = new CustomDestination<MySimpleRow>(
                row => rows.Add(row)
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
            //Act
            string json = JsonConvert.SerializeObject(rows, Formatting.Indented);

            //Assert
            Assert.Equal(File.ReadAllText("res/CustomDestination/simpleJson_tobe.json"), json);
        }
    }
}
