using System.Linq;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBMerge
{
    [Collection("DataFlow")]
    public class DbMergeDeltaTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public static SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        public class MyMergeRow : MergeableRow
        {
            [IdColumn]
            [ColumnMap("Col1")]
            public long Key { get; set; }

            [CompareColumn]
            [ColumnMap("Col2")]
            public string Value { get; set; }

            [DeleteColumn(true)]
            public bool DeleteThisRow { get; set; }
        }

        [Theory, MemberData(nameof(Connections))]
        public void DeltaLoadWithDeletion(IConnectionManager connection)
        {
            //Arrange
            MemorySource<MyMergeRow> source = new MemorySource<MyMergeRow>();
            source.DataAsList.Add(new MyMergeRow { Key = 2, Value = "Test2" });
            source.DataAsList.Add(new MyMergeRow { Key = 3, Value = "Test3" });
            source.DataAsList.Add(new MyMergeRow { Key = 4, DeleteThisRow = true });
            source.DataAsList.Add(new MyMergeRow { Key = 10, DeleteThisRow = true });
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(
                connection,
                "DBMergeDeltaDestination"
            );
            d2c.InsertTestDataSet3();

            //Act
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>(
                connection,
                "DBMergeDeltaDestination"
            )
            {
                DeltaMode = DeltaMode.Delta
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.True(dest.UseTruncateMethod == false);
            d2c.AssertTestData();
            Assert.True(dest.DeltaTable.Count == 4);
            Assert.True(
                dest.DeltaTable.Count(
                    row => row.ChangeAction == ChangeAction.Update && row.Key == 2
                ) == 1
            );
            Assert.True(
                dest.DeltaTable.Count(
                    row => row.ChangeAction == ChangeAction.Insert && row.Key == 3
                ) == 1
            );
            Assert.True(
                dest.DeltaTable.Count(
                    row => row.ChangeAction == ChangeAction.Delete && row.Key == 4
                ) == 1
            );
            Assert.True(
                dest.DeltaTable.Count(
                    row => row.ChangeAction == ChangeAction.Delete && row.Key == 10
                ) == 1
            );
        }
    }
}
