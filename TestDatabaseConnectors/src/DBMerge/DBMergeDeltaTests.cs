using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.DataFlow;
using ALE.ETLBox.src.Definitions.DataFlow.Type;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestDatabaseConnectors.src.Fixtures;
using TestShared.src.SharedFixtures;

namespace TestDatabaseConnectors.src.DBMerge
{
    public class DbMergeDeltaTests : DatabaseConnectorsTestBase
    {
        public DbMergeDeltaTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

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
            var source = new MemorySource<MyMergeRow>();
            source.DataAsList.Add(new MyMergeRow { Key = 2, Value = "Test2" });
            source.DataAsList.Add(new MyMergeRow { Key = 3, Value = "Test3" });
            source.DataAsList.Add(new MyMergeRow { Key = 4, DeleteThisRow = true });
            source.DataAsList.Add(new MyMergeRow { Key = 10, DeleteThisRow = true });
            var d2C = new TwoColumnsTableFixture(
                connection,
                "DBMergeDeltaDestination"
            );
            d2C.InsertTestDataSet3();

            //Act
            var dest = new DbMerge<MyMergeRow>(
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
            Assert.False(dest.UseTruncateMethod);
            d2C.AssertTestData();
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
