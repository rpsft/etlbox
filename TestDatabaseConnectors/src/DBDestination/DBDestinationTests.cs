using ETLBox.Connection;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.DataFlow.Transformations;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbDestinationTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public DbDestinationTests(DataFlowDatabaseFixture dbFixture)
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

        [Theory, MemberData(nameof(Connections))]
        public void ColumnMapping(IConnectionManager connection)
        {
            //Arrange
            FourColumnsTableFixture source4Columns = new FourColumnsTableFixture(connection, "Source");
            source4Columns.InsertTestData();
            FourColumnsTableFixture dest4Columns = new FourColumnsTableFixture(connection, "Destination", identityColumnIndex: 2);

            DbSource<string[]> source = new DbSource<string[]>(connection, "Source");
            RowTransformation<string[], MyExtendedRow> trans = new RowTransformation<string[], MyExtendedRow>(
                row => new MyExtendedRow()
                {
                    Id = int.Parse(row[0]),
                    Text = row[1],
                    Value = row[2] != null ? (long?)long.Parse(row[2]) : null,
                    Percentage = decimal.Parse(row[3])
                });

            //Act
            DbDestination<MyExtendedRow> dest = new DbDestination<MyExtendedRow>(connection, "Destination");
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest4Columns.AssertTestData();

        }
    }
}
