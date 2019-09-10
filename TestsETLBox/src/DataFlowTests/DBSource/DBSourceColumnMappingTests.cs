using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DBSourceColumnMappingTests : IDisposable
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DBSourceColumnMappingTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class ColumnMapRow
        {
            public long Col1 { get; set; }
            [ColumnMap("Col2")]
            public string B { get; set; }

        }

        [Theory, MemberData(nameof(Connections))]
        public void ColumnMapping(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "Source");
            source2Columns.InsertTestData();

            //Act
            DBSource<ColumnMapRow> source = new DBSource<ColumnMapRow>(connection, "Source");
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
