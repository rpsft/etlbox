using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests.SqlServer
{
    [Collection("Sql Server DataFlow")]
    public class DBSourceColumnMappingTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public DBSourceColumnMappingTests(DatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class ColumnMapRow
        {
            public int Col1 { get; set; }
            [ColumnMap("Col2")]
            public string B { get; set; }

        }

        [Fact]
        public void ColumnMapping()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("Source");
            source2Columns.InsertTestData();

            //Act
            DBSource<ColumnMapRow> source = new DBSource<ColumnMapRow>(Connection, "Source");
            CustomDestination<ColumnMapRow> dest = new CustomDestination<ColumnMapRow>(
                input =>
                {
                    //Assert
                    Assert.InRange(input.Col1, 1, 3);
                    Assert.StartsWith("Test", input.B);
                });
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
        }



    }
}
