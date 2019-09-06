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
    public class DBMergeTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("DataFlow");
        public DBMergeTests(DatabaseFixture dbFixture)
        {
        }

        public void Dispose()
        {
        }

        public class MyMergeRow : IMergable
        {
            [ColumnMap("Col1")]
            public int Key { get; set; }
            [ColumnMap("Col2")]
            public string Value { get; set; }

            /* IMergable interface */
            public DateTime ChangeDate { get; set; }
            public char ChangeAction { get; set; }

            [MergeIdColumnName("Col1")]
            public string UniqueId => Key.ToString();

            public override bool Equals(object other)
            {
                var msr = other as MyMergeRow;
                if (other == null) return false;
                return msr.Value == this.Value;
            }
        }

        [Fact]
        public void SimpleMerge()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("DBMergeSource");
            source2Columns.InsertTestData();
            source2Columns.InsertTestDataSet2();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("DBMergeDestination");
            dest2Columns.InsertTestDataSet3();
            DBSource<MyMergeRow> source = new DBSource<MyMergeRow>(Connection, "DBMergeSource");

            //Act
            DBMerge<MyMergeRow> dest = new DBMerge<MyMergeRow>(Connection, "DBMergeDestination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(6, RowCountTask.Count(Connection, "DBMergeDestination", "Col1 BETWEEN 1 AND 7 AND Col2 LIKE 'Test%'"));
            Assert.True(dest.DeltaTable.Count == 7);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == 'U').Count() == 2);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == 'D' && row.Key == 10).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == 'I').Count() == 3);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == 'E' && row.Key == 1).Count() == 1);
        }

        public class MySimpleRow : IMergable
        {
            [ColumnMap("Col1")]
            public int Key { get; set; }
            [ColumnMap("Col2")]
            public string Value { get; set; }
            public DateTime ChangeDate { get; set; }
            public char ChangeAction { get; set; }
            public string UniqueId => Key.ToString();
        }

        [Fact]
        public void NoMergeIdColumn()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("DBMergeSource");
            source2Columns.InsertTestData();
            source2Columns.InsertTestDataSet2();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("DBMergeDestination");
            dest2Columns.InsertTestDataSet3();
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(Connection, "DBMergeSource");

            //Act
            DBMerge<MySimpleRow> dest = new DBMerge<MySimpleRow>(Connection, "DBMergeDestination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(6, RowCountTask.Count(Connection, "DBMergeDestination", "Col1 BETWEEN 1 AND 7 AND Col2 LIKE 'Test%'"));
            Assert.True(dest.DeltaTable.Count == 7);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == 'U').Count() == 3);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == 'D' && row.Key == 10).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == 'I').Count() == 3);
        }

        [Fact]
        public void WithDeltaDestination()
        {
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture("DBMergeSource");
            source2Columns.InsertTestData();
            source2Columns.InsertTestDataSet2();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("DBMergeDestination");
            dest2Columns.InsertTestDataSet3();
            TwoColumnsDeltaTableFixture delta2Columns = new TwoColumnsDeltaTableFixture("DBMergeDelta");

            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(Connection, "DBMergeSource");

            //Act
            DBMerge<MySimpleRow> merge = new DBMerge<MySimpleRow>(Connection, "DBMergeDestination");
            DBDestination<MySimpleRow> delta = new DBDestination<MySimpleRow>(Connection, "DBMergeDelta");
            source.LinkTo(merge);
            merge.LinkTo(delta);
            source.Execute();
            merge.Wait();
            delta.Wait();

            //Assert
            Assert.Equal(6, RowCountTask.Count(Connection, "DBMergeDestination", "Col1 BETWEEN 1 AND 7 AND Col2 LIKE 'Test%'"));
            Assert.Equal(7, RowCountTask.Count(Connection, "DBMergeDelta", "Col1 BETWEEN 1 AND 10 AND Col2 LIKE 'Test%'"));
            Assert.Equal(1, RowCountTask.Count(Connection, "DBMergeDelta", "ChangeAction = 'D' AND Col1 = 10"));
            Assert.Equal(3, RowCountTask.Count(Connection, "DBMergeDelta", "ChangeAction = 'U' AND Col1 IN (1,2,4)"));
            Assert.Equal(3, RowCountTask.Count(Connection, "DBMergeDelta", "ChangeAction = 'I' AND Col1 IN (3,5,6)"));
        }
    }
}
