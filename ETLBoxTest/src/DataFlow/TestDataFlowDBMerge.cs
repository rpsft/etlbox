using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestDataFlowDBMerge {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void ClassInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateSchemaTask.Create("test");
        }

        [TestInitialize]
        public void TestInit() {
            CleanUpSchemaTask.CleanUp("test");
        }

        public class MySimpleRow : IMergable
        {
            [ColumnMap("ColKey")]
            public int Key { get; set; }
            [ColumnMap("ColValue")]
            public string Value { get; set; }

            /* IMergable interface */
            public DateTime ChangeDate { get; set; }
            public char ChangeAction { get; set; }

            [MergeIdColumnName("ColKey")]
            public string UniqueId => Key.ToString();

            public override bool Equals(object other)
            {
                var msr = other as MySimpleRow;
                if (other == null) return false;
                return msr.Value == this.Value;
            }
        }

        public class MySimpleRowNoMergeIdColumn : IMergable
        {
            [ColumnMap("ColKey")]
            public int Key { get; set; }
            [ColumnMap("ColValue")]
            public string Value { get; set; }
            public DateTime ChangeDate { get; set; }
            public char ChangeAction { get; set; }

            public string UniqueId => Key.ToString();
        }


        [TestMethod]
        public void DBMerge()
        {
            CreateSourceTable();
            CreateDestinationTable();

            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>("test.Source");
            DBMerge<MySimpleRow> dest = new DBMerge<MySimpleRow>("test.Destination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            AssertDestinationTable();
        }

        [TestMethod]
        public void DBMergeNoMergeIdColumn()
        {
            CreateSourceTable();
            CreateDestinationTable();

            DBSource<MySimpleRowNoMergeIdColumn> source = new DBSource<MySimpleRowNoMergeIdColumn>("test.Source");
            DBMerge<MySimpleRowNoMergeIdColumn> dest = new DBMerge<MySimpleRowNoMergeIdColumn>("test.Destination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            AssertDestinationTable();
        }

        private static void AssertDestinationTable()
        {
            Assert.AreEqual(4, RowCountTask.Count("test.Destination"));
            Assert.AreEqual(1, RowCountTask.Count("test.Destination", "ColValue = 'Test1' AND ColKey=1"));
            Assert.AreEqual(1, RowCountTask.Count("test.Destination", "ColValue = 'Test2' AND ColKey=2"));
            Assert.AreEqual(1, RowCountTask.Count("test.Destination", "ColValue = 'Test3' AND ColKey=3"));
            Assert.AreEqual(1, RowCountTask.Count("test.Destination", "ColValue = 'Test4' AND ColKey=4"));
        }

        private static void CreateDestinationTable()
        {
            SqlTask.ExecuteNonQuery("Create destination table", @"CREATE TABLE test.Destination
                (ID int not null identity (10,10) primary key,
                 ColKey int not null, ColValue nvarchar(30) null, ColValue2 nvarchar(10))");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Destination (ColKey, ColValue) values(1,'Test')");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Destination (ColKey, ColValue) values(2,NULL)");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Destination (ColKey, ColValue, ColValue2) values(3,'Test3', 'xxx')");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Destination (ColKey, ColValue) values(5,'Test5')");
        }

        private static void CreateSourceTable()
        {
            SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Source
                (ColKey int not null, ColValue nvarchar(50) null)");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values(1,'Test1')");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values(2,'Test2')");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values(3,'Test3')");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values(4,'Test4')");
        }
    }

}
