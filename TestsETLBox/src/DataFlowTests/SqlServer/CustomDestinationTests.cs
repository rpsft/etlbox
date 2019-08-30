using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests.SqlServer
{
    [Collection("Sql Server DataFlow")]
    public class CustomDestinationTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public CustomDestinationTests(DatabaseFixture dbFixture)
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
        public void InsertIntoTable()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("Source");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("CustomDestination");

            //Act
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(Connection, "Source");
            CustomDestination<MySimpleRow> dest = new CustomDestination<MySimpleRow>(
                row => {
                    SqlTask.ExecuteNonQuery(Connection, "Insert row",
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
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(Connection, "JSonSource");
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
