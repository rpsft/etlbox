using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ETLBox.Primitives;

namespace TestDatabaseConnectors.DBMerge
{
    public class DbMergeTests : DatabaseConnectorsTestBase
    {
        public DbMergeTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
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
        public void SimpleMerge(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture(connection, "DBMergeSource");
            s2C.InsertTestData();
            s2C.InsertTestDataSet2();
            TwoColumnsTableFixture d2C = new TwoColumnsTableFixture(
                connection,
                "DBMergeDestination"
            );
            d2C.InsertTestDataSet3();
            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(connection, "DBMergeSource");

            //Act
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>(connection, "DBMergeDestination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                6,
                RowCountTask.Count(
                    connection,
                    "DBMergeDestination",
                    $"{d2C.QB}Col1{d2C.QE} BETWEEN 1 AND 7 AND {d2C.QB}Col2{d2C.QE} LIKE 'Test%'"
                )
            );
            Assert.True(dest.DeltaTable.Count == 7);
            Assert.True(dest.DeltaTable.Count(row => row.ChangeAction == ChangeAction.Update) == 2);
            Assert.True(
                dest.DeltaTable.Count(
                    row => row.ChangeAction == ChangeAction.Delete && row.Key == 10
                ) == 1
            );
            Assert.True(dest.DeltaTable.Count(row => row.ChangeAction == ChangeAction.Insert) == 3);
            Assert.True(
                dest.DeltaTable.Count(
                    row => row.ChangeAction == ChangeAction.Exists && row.Key == 1
                ) == 1
            );
        }

        [Theory, MemberData(nameof(Connections))]
        public void DisablingDeletion(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture(connection, "DBMergeSource");
            s2C.InsertTestData();
            TwoColumnsTableFixture d2C = new TwoColumnsTableFixture(
                connection,
                "DBMergeDestination"
            );
            d2C.InsertTestDataSet3();
            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(connection, "DBMergeSource");

            //Act
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>(connection, "DBMergeDestination")
            {
                DeltaMode = DeltaMode.NoDeletions
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(5, RowCountTask.Count(connection, "DBMergeDestination"));
            Assert.True(dest.DeltaTable.Count == 3);
            Assert.True(
                dest.DeltaTable.Count(
                    row => row.ChangeAction == ChangeAction.Insert && row.Key == 3
                ) == 1
            );
            Assert.True(
                dest.DeltaTable.Count(
                    row => row.ChangeAction == ChangeAction.Update && row.Key == 2
                ) == 1
            );
            Assert.True(
                dest.DeltaTable.Count(
                    row => row.ChangeAction == ChangeAction.Exists && row.Key == 1
                ) == 1
            );
        }

        [Theory, MemberData(nameof(Connections))]
        public void EnforcingTruncate(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture(connection, "DBMergeSource");
            s2C.InsertTestData();
            TwoColumnsTableFixture d2C = new TwoColumnsTableFixture(
                connection,
                "DBMergeDestination"
            );
            d2C.InsertTestDataSet3();
            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(connection, "DBMergeSource");

            //Act
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>(connection, "DBMergeDestination")
            {
                UseTruncateMethod = true
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "DBMergeDestination"));
            Assert.True(dest.DeltaTable.Count == 5);
            Assert.True(
                dest.DeltaTable.Count(
                    row => row.ChangeAction == ChangeAction.Exists && row.Key == 1
                ) == 1
            );
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

        [Fact]
        public void MergeIntoEmptyDestination()
        {
            //Arrange
            TwoColumnsTableFixture s2C = new TwoColumnsTableFixture(
                SqlConnection,
                "DBMergeEmptySource"
            );
            s2C.InsertTestData();
            TwoColumnsTableFixture d2C = new TwoColumnsTableFixture(
                SqlConnection,
                "DBMergeEmptyDestination"
            );
            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(
                SqlConnection,
                "DBMergeEmptySource"
            );

            //Act
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>(
                SqlConnection,
                "DBMergeEmptyDestination"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2C.AssertTestData();
        }

        [Fact]
        public void MergeFromEmptySource()
        {
            //Arrange
            var _ = new TwoColumnsTableFixture(SqlConnection, "DBMergeEmptySource");
            TwoColumnsTableFixture d2C = new TwoColumnsTableFixture(
                SqlConnection,
                "DBMergeEmptyDestination"
            );
            d2C.InsertTestData();
            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(
                SqlConnection,
                "DBMergeEmptySource"
            );

            //Act
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>(
                SqlConnection,
                "DBMergeEmptyDestination"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2C.AssertTestData();
        }
    }
}
