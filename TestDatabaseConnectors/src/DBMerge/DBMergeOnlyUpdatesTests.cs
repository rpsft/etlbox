using ETLBox.Connection;
using ETLBox.ControlFlow.Tasks;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbMergeOnlyUpdatesTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        //public static SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        public DbMergeOnlyUpdatesTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public class MyMergeRow : MergeableRow
        {
            [IdColumn]
            [ColumnMap("Col1")]
            public long Key { get; set; }

            [CompareColumn]
            [ColumnMap("Col2")]
            public string Value { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void OnlyUpdates(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "DBMergeOnlyUpdatesSource");
            s2c.InsertTestData();
            s2c.InsertTestDataSet2();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "DBMergeOnlyUpdatesDestination");
            d2c.InsertTestDataSet3();
            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(connection, "DBMergeOnlyUpdatesSource");

            //Act
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>(connection, "DBMergeOnlyUpdatesDestination");
            dest.MergeMode = MergeMode.OnlyUpdates;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(4, RowCountTask.Count(connection, "DBMergeOnlyUpdatesDestination", $"{d2c.QB}Col1{d2c.QE} BETWEEN 1 AND 10 AND {d2c.QB}Col2{d2c.QE} LIKE 'Test%'"));
            Assert.True(dest.DeltaTable.Count == 3);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Update).Count() == 2);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Exists && row.Key == 1).Count() == 1);
        }

    }
}
