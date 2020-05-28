using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBox.SqlServer;
using ETLBoxTests.Fixtures;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbMergeTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");
        public static SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        public DbMergeTests(DataFlowDatabaseFixture dbFixture)
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
        public void SimpleMerge(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "DBMergeSource");
            s2c.InsertTestData();
            s2c.InsertTestDataSet2();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "DBMergeDestination");
            d2c.InsertTestDataSet3();
            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(connection, "DBMergeSource");

            //Act
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>(connection, "DBMergeDestination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(6, RowCountTask.Count(connection, "DBMergeDestination", $"{d2c.QB}Col1{d2c.QE} BETWEEN 1 AND 7 AND {d2c.QB}Col2{d2c.QE} LIKE 'Test%'"));
            Assert.True(dest.DeltaTable.Count == 7);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Update).Count() == 2);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Delete && row.Key == 10).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Insert).Count() == 3);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Exists && row.Key == 1).Count() == 1);
        }

        [Theory, MemberData(nameof(Connections))]
        public void DisablingDeletion(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "DBMergeSource");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "DBMergeDestination");
            d2c.InsertTestDataSet3();
            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(connection, "DBMergeSource");

            //Act
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>(connection, "DBMergeDestination");
            dest.DeltaMode = DeltaMode.NoDeletions;
            //dest.DisableDeletion = true;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(5, RowCountTask.Count(connection, "DBMergeDestination"));
            Assert.True(dest.DeltaTable.Count == 3);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Insert && row.Key == 3).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Update && row.Key == 2).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Exists && row.Key == 1).Count() == 1);
        }

        [Theory, MemberData(nameof(Connections))]
        public void EnforcingTruncate(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "DBMergeSource");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "DBMergeDestination");
            d2c.InsertTestDataSet3();
            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(connection, "DBMergeSource");

            //Act
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>(connection, "DBMergeDestination");
            dest.UseTruncateMethod = true;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "DBMergeDestination"));
            Assert.True(dest.DeltaTable.Count == 5);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Exists && row.Key == 1).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Update && row.Key == 2).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Insert && row.Key == 3).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Delete && row.Key == 4).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Delete && row.Key == 10).Count() == 1);
        }

        [Fact]
        public void MergeIntoEmptyDestination()
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(SqlConnection, "DBMergeEmptySource");
            s2c.InsertTestData();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(SqlConnection, "DBMergeEmptyDestination");
            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(SqlConnection, "DBMergeEmptySource");

            //Act
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>(SqlConnection, "DBMergeEmptyDestination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }

        [Fact]
        public void MergeFromEmptySource()
        {
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(SqlConnection, "DBMergeEmptySource");
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(SqlConnection, "DBMergeEmptyDestination");
            d2c.InsertTestData();
            DbSource<MyMergeRow> source = new DbSource<MyMergeRow>(SqlConnection, "DBMergeEmptySource");

            //Act
            DbMerge<MyMergeRow> dest = new DbMerge<MyMergeRow>(SqlConnection, "DBMergeEmptyDestination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
        }
    }
}
