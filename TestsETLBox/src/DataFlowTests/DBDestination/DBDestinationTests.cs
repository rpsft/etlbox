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
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DBDestinationTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public DBDestinationTests(DatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }


        public class MyExtendedRow
        {
            [ColumnMap("Col1")]
            public int Id { get; set; }
            [ColumnMap("Col3")]
            public long? Value { get; set; }
            [ColumnMap("Col4")]
            public decimal Percentage { get; set; }
            [ColumnMap("Col2")]
            public string Text { get; set; }
        }

        [Fact]
        public void ColumnMapping()
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture("Source");
            source4Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture("Destination", identityColumnIndex: 2);

            DBSource source = new DBSource(Connection, "dbo.Source");
            RowTransformation<string[], MyExtendedRow> trans = new RowTransformation<string[], MyExtendedRow>(
                row => new MyExtendedRow()
                {
                    Id = int.Parse(row[0]),
                    Text = row[1],
                    Value = row[2] != null ? (long?)long.Parse(row[2]) : null,
                    Percentage = decimal.Parse(row[3])
                });

            //Act
            DBDestination<MyExtendedRow> dest = new DBDestination<MyExtendedRow>(Connection, "dbo.Destination");
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();

        }
    }
}
