using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DBMergeTests : IDisposable
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnectionManager("DataFlow");
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        public DBMergeTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class MyMergeRow : IMergable
        {
            [ColumnMap("Col1")]
            public long Key { get; set; }
            [ColumnMap("Col2")]
            public string Value { get; set; }

            /* IMergable interface */
            public DateTime ChangeDate { get; set; }
            public string ChangeAction { get; set; }

            [MergeIdColumnName("Col1")]
            public string UniqueId => Key.ToString();

            public override bool Equals(object other)
            {
                var msr = other as MyMergeRow;
                if (other == null) return false;
                return msr.Value == this.Value;
            }
        }

        [Theory, MemberData(nameof(Connections))]
        public void SimpleMerge(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "DBMergeSource");
            source2Columns.InsertTestData();
            source2Columns.InsertTestDataSet2();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DBMergeDestination");
            dest2Columns.InsertTestDataSet3();
            DBSource<MyMergeRow> source = new DBSource<MyMergeRow>(connection, "DBMergeSource");

            //Act
            DBMerge<MyMergeRow> dest = new DBMerge<MyMergeRow>(connection, "DBMergeDestination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(6, RowCountTask.Count(connection, "DBMergeDestination", "Col1 BETWEEN 1 AND 7 AND Col2 LIKE 'Test%'"));
            Assert.True(dest.DeltaTable.Count == 7);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "U").Count() == 2);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "D" && row.Key == 10).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "I").Count() == 3);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "E" && row.Key == 1).Count() == 1);
        }

        public class MySimpleRow : IMergable
        {
            [ColumnMap("Col1")]
            public long Key { get; set; }
            [ColumnMap("Col2")]
            public string Value { get; set; }
            public DateTime ChangeDate { get; set; }
            public string ChangeAction { get; set; }
            public string UniqueId => Key.ToString();
        }

        [Theory, MemberData(nameof(Connections))]
        public void NoMergeIdColumn(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection,"DBMergeSource");
            source2Columns.InsertTestData();
            source2Columns.InsertTestDataSet2();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection,"DBMergeDestination");
            dest2Columns.InsertTestDataSet3();
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(connection, "DBMergeSource");

            //Act
            DBMerge<MySimpleRow> dest = new DBMerge<MySimpleRow>(connection, "DBMergeDestination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(6, RowCountTask.Count(connection, "DBMergeDestination", "Col1 BETWEEN 1 AND 7 AND Col2 LIKE 'Test%'"));
            Assert.True(dest.DeltaTable.Count == 7);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "U").Count() == 3);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "D" && row.Key == 10).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "I").Count() == 3);
        }

        [Theory, MemberData(nameof(Connections))]
        public void WithDeltaDestination(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(connection, "DBMergeSource");
            source2Columns.InsertTestData();
            source2Columns.InsertTestDataSet2();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(connection, "DBMergeDestination");
            dest2Columns.InsertTestDataSet3();
            TwoColumnsDeltaTableFixture delta2Columns = new TwoColumnsDeltaTableFixture(connection,"DBMergeDelta");

            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(connection, "DBMergeSource");

            //Act
            DBMerge<MySimpleRow> merge = new DBMerge<MySimpleRow>(connection, "DBMergeDestination");
            DBDestination<MySimpleRow> delta = new DBDestination<MySimpleRow>(connection, "DBMergeDelta");
            source.LinkTo(merge);
            merge.LinkTo(delta);
            source.Execute();
            merge.Wait();
            delta.Wait();

            //Assert
            Assert.Equal(6, RowCountTask.Count(connection, "DBMergeDestination", "Col1 BETWEEN 1 AND 7 AND Col2 LIKE 'Test%'"));
            Assert.Equal(7, RowCountTask.Count(connection, "DBMergeDelta", "Col1 BETWEEN 1 AND 10 AND Col2 LIKE 'Test%'"));
            Assert.Equal(1, RowCountTask.Count(connection, "DBMergeDelta", "ChangeAction = 'D' AND Col1 = 10"));
            Assert.Equal(3, RowCountTask.Count(connection, "DBMergeDelta", "ChangeAction = 'U' AND Col1 IN (1,2,4)"));
            Assert.Equal(3, RowCountTask.Count(connection, "DBMergeDelta", "ChangeAction = 'I' AND Col1 IN (3,5,6)"));
        }
    }
}
