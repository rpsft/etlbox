//using ALE.ETLBox;
//using ALE.ETLBox.ConnectionManager;
//using ALE.ETLBox.ControlFlow;
//using ALE.ETLBox.DataFlow;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using System.Collections.Generic;

//namespace ALE.ETLBoxTest {
//    [TestClass]
//    public class TestDataFlowDBDestination
//    {
//        public TestContext TestContext { get; set; }
//        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
//        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

//        [ClassInitialize]
//        public static void ClassInit(TestContext testContext)
//        {
//            TestHelper.RecreateDatabase(testContext);
//            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
//            CreateSchemaTask.Create("test");
//        }

//        [TestInitialize]
//        public void TestInit()
//        {
//            CleanUpSchemaTask.CleanUp("test");
//        }

//        [TestMethod]
//        public void DBDestinationWithColumnMapping()
//        {
//            SqlTask.ExecuteNonQuery("Create destination table", @"CREATE TABLE test.Destination
//                (ID int not null identity (1,1) primary key,
//                 Col1 nvarchar(30) null, Col2 nvarchar(30) null)");

//            int index = 1;
//            CustomSource<ColumnMapRow> source = new CustomSource<ColumnMapRow>(() =>
//            {
//                return new ColumnMapRow()
//                {
//                    Col1 = "Test" + index++,
//                    B = "Test2",
//                };
//            }, () => index > 3);
//            DBDestination<ColumnMapRow> dest = new DBDestination<ColumnMapRow>("test.Destination");
//            source.LinkTo(dest);
//            source.Execute();
//            dest.Wait();
//            Assert.AreEqual(3, RowCountTask.Count("test.Destination", "Col2 = 'Test2'"));
//        }

//        public class ColumnMapRow
//        {
//            public string None { get; set; } = "XXX";
//            public string Col1 { get; set; }
//            [ColumnMap("Col2")]
//            public string B { get; set; }

//        }

//        [TestMethod]
//        public void DBSourceWithColumnMapping()
//        {
//            SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Source
//                (Col1 nvarchar(100) null, Col2 nvarchar(100) null)");
//            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");
//            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");
//            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");

//            DBSource<ColumnMapRow> source = new DBSource<ColumnMapRow>("test.Source");
//            CustomDestination<ColumnMapRow> dest = new CustomDestination<ColumnMapRow>(
//                input =>
//                {
//                    Assert.AreEqual("Test1", input.Col1);
//                    Assert.AreEqual("Test2", input.B);
//                });
//            source.LinkTo(dest);
//            source.Execute();
//            dest.Wait();
//        }

//        //Add Tests: DBDestination with column definition!!!
//    }
//}
