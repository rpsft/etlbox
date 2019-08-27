//using ALE.ETLBox;
//using ALE.ETLBox.ConnectionManager;
//using ALE.ETLBox.ControlFlow;
//using ALE.ETLBox.DataFlow;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using System.Collections.Generic;

//namespace ALE.ETLBoxTest
//{
//    [TestClass]
//    public class TestDataFlowDBSource
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
//            CleanUpSchemaTask.CleanUp("dbo");
//        }

//        public class MySimpleRow
//        {
//            public string Col1 { get; set; }
//            public int Col2 { get; set; }
//        }

//        /*
//         * DSBSource (out: object) -> DBDestination (in: object)
//         */
//        //[TestMethod]
//        //public void DB_DB()
//        //{
//        //    TableDefinition sourceTableDefinition = new TableDefinition("test.Source", new List<TableColumn>() {
//        //        new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
//        //        new TableColumn("Col2", "int", allowNulls: true)
//        //    });
//        //    sourceTableDefinition.CreateTable();
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1',1)");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test2',2)");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test3',3)");

//        //    TableDefinition destinationTableDefinition = new TableDefinition("test.Destination", new List<TableColumn>() {
//        //        new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
//        //        new TableColumn("Col2", "int", allowNulls: true)
//        //    });
//        //    destinationTableDefinition.CreateTable();

//        //    DBSource<MySimpleRow> source = new DBSource<MySimpleRow>() { SourceTableDefinition = sourceTableDefinition };
//        //    DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>() { DestinationTableDefinition = destinationTableDefinition };
//        //    source.LinkTo(dest);
//        //    source.Execute();
//        //    dest.Wait();
//        //    Assert.AreEqual(3, RowCountTask.Count("test.Destination"));
//        //    Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col1 = 'Test1' AND Col2=1"));
//        //    Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col1 = 'Test2' AND Col2=2"));
//        //    Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col1 = 'Test3' AND Col2=3"));
//        //}

//        /*
//         * DSBSource (out: object) -> DBDestination (in: object)
//         */
//        //[TestMethod]
//        //public void Sql_DB_withSelectStar()
//        //{
//        //    TableDefinition destinationTableDefinition = new TableDefinition("test.Destination", new List<TableColumn>() {
//        //        new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
//        //        new TableColumn("Col2", "int", allowNulls: true)
//        //    });
//        //    destinationTableDefinition.CreateTable();

//        //    DBSource<MySimpleRow> source = new DBSource<MySimpleRow>()
//        //    {
//        //        Sql = $@"select * from (values ('Test1',1), ('Test2',2), ('Test3',3)) AS MyTable(Col1,Col2)"
//        //    };

//        //    DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>() { DestinationTableDefinition = destinationTableDefinition };
//        //    source.LinkTo(dest);
//        //    source.Execute();
//        //    dest.Wait();
//        //    Assert.AreEqual(3, RowCountTask.Count("test.Destination"));
//        //    Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col1 = 'Test1' AND Col2=1"));
//        //    Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col1 = 'Test2' AND Col2=2"));
//        //    Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col1 = 'Test3' AND Col2=3"));
//        //}

//        /*
//       * DSBSource (out: object) -> DBDestination (in: object)
//       */
//        //[TestMethod]
//        //public void Sql_Tablename_WithSelectStar()
//        //{
//        //    TableDefinition destinationTableDefinition = new TableDefinition("test.Destination", new List<TableColumn>() {
//        //        new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
//        //        new TableColumn("Col2", "int", allowNulls: true)
//        //    });
//        //    destinationTableDefinition.CreateTable();

//        //    DBSource<MySimpleRow> source = new DBSource<MySimpleRow>()
//        //    {
//        //        Sql = $@"select * from (values ('Test1',1), ('Test2',2), ('Test',3)) AS MyTable(Col1,Col2)"
//        //    };
//        //    DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>("test.Destination");
//        //    source.LinkTo(dest);
//        //    source.Execute();
//        //    dest.Wait();
//        //    Assert.AreEqual(3, RowCountTask.Count("test.Destination"));
//        //}

//        /*
//* DSBSource (out: object) -> DBDestination (in: object)
//*/
//        //[TestMethod]
//        //public void DboTableName_DboTablename()
//        //{
//        //    SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE Source
//        //        (Col1 nvarchar(100) null, Col2 int null)");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into Source values('Test1',1)");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into Source values('Test2',2)");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into Source values('Test3',3)");
//        //    SqlTask.ExecuteNonQuery("Create destination table", @"CREATE TABLE Destination
//        //        (Col1 nvarchar(30) null, Col2 bigint null)");

//        //    DBSource<MySimpleRow> source = new DBSource<MySimpleRow>("Source");
//        //    DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>("Destination");
//        //    source.LinkTo(dest);
//        //    source.Execute();
//        //    dest.Wait();
//        //    Assert.AreEqual(3, RowCountTask.Count("Destination"));
//        //    Assert.AreEqual(1, RowCountTask.Count("Destination", "Col1 = 'Test1' AND Col2=1"));
//        //    Assert.AreEqual(1, RowCountTask.Count("Destination", "Col1 = 'Test2' AND Col2=2"));
//        //    Assert.AreEqual(1, RowCountTask.Count("Destination", "Col1 = 'Test3' AND Col2=3"));
//        //}

//        //public class MyExtendedRow
//        //{
//        //    public string Col2 { get; set; }
//        //    public decimal? Col4 { get; set; }
//        //}

//        //[TestMethod]
//        //public void MatchingPropertyNames_IDColumnAtBeginning()
//        //{
//        //    SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Source
//        //        (Col1 int not null identity(1,1) primary key, Col2 nvarchar(20) not null, Col3 int null, Col4 decimal(12,4) null)");
//        //    SqlTask.ExecuteNonQuery("Create destination table", @"CREATE TABLE test.Destination
//        //       (Col1 int not null identity(1,1) primary key, Col2 nvarchar(20) not null, Col3 int null, Col4 decimal(12,4) null)");
//        //    ExecuteDataFlow_ExtendedRows();
//        //}

//        //private static void ExecuteDataFlow_ExtendedRows()
//        //{
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source (Col2, Col4) values('Test1', '2.5')");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source (Col2, Col4) values('Test2', '12.5')");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source (Col2, Col4) values('Test3', '123.5')");

//        //    DBSource<MyExtendedRow> source = new DBSource<MyExtendedRow>("test.Source");
//        //    DBDestination<MyExtendedRow> dest = new DBDestination<MyExtendedRow>("test.Destination");
//        //    source.LinkTo(dest);
//        //    source.Execute();
//        //    dest.Wait();
//        //    Assert.AreEqual(3, RowCountTask.Count("test.Destination"));
//        //    Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col2 = 'Test1' AND Col4='2.5'"));
//        //    Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col2 = 'Test2' AND Col4='12.5'"));
//        //    Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col2 = 'Test3' AND Col4='123.5'"));
//        //}

//        //[TestMethod]
//        //public void MatchingPropertyNames_IDColumnInTheMiddle()
//        //{
//        //    SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Source
//        //        (Col2 nvarchar(20) not null, Col1 int not null identity(1,1) primary key, Col4 decimal(12,2) null)");
//        //    SqlTask.ExecuteNonQuery("Create destination table", @"CREATE TABLE test.Destination
//        //        (Col2 nvarchar(20) not null, Col1 int not null identity(1,1) primary key, Col4 decimal(12,2) null)");

//        //    ExecuteDataFlow_ExtendedRows();
//        //}

//        //[TestMethod]
//        //public void MatchingPropertyNames_IDColumnAtTheEnd()
//        //{
//        //    SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Source
//        //        (Col2 nvarchar(20) not null, Col3 int null, Col4 decimal(12,2) null, Col1 int not null identity(1,1) primary key)");
//        //    SqlTask.ExecuteNonQuery("Create destination table", @"CREATE TABLE test.Destination
//        //        (Col2 nvarchar(20) not null, Col3 int null, Col4 decimal(12,2) null, Col1 int not null identity(1,1) primary key)");

//        //    ExecuteDataFlow_ExtendedRows();
//        //}

//        //[TestMethod]
//        //public void DBSourceWithStringArray()
//        //{
//        //    SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Source
//        //        (Col1 nvarchar(100) null, Col2 int null)");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1',1)");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test2',2)");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test3',3)");
//        //    SqlTask.ExecuteNonQuery("Create destination table", @"CREATE TABLE test.Destination
//        //        (Col1 nvarchar(30) null, Col2 bigint null)");

//        //    DBSource source = new DBSource("test.Source");
//        //    DBDestination dest = new DBDestination("test.Destination");
//        //    source.LinkTo(dest);
//        //    source.Execute();
//        //    dest.Wait();
//        //    Assert.AreEqual(3, RowCountTask.Count("test.Destination"));
//        //    Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col1 = 'Test1' AND Col2=1"));
//        //    Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col1 = 'Test2' AND Col2=2"));
//        //    Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col1 = 'Test3' AND Col2=3"));
//        //}

//        //public class ColumnMapRow
//        //{
//        //    public string Col1 { get; set; }
//        //    [ColumnMap("Col2")]
//        //    public string B { get; set; }

//        //}

//        //[TestMethod]
//        //public void DBSourceWithColumnMapping()
//        //{
//        //    SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Source
//        //        (Col1 nvarchar(100) null, Col2 nvarchar(100) null)");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");

//        //    DBSource<ColumnMapRow> source = new DBSource<ColumnMapRow>("test.Source");
//        //    CustomDestination<ColumnMapRow> dest = new CustomDestination<ColumnMapRow>(
//        //        input =>
//        //        {
//        //            Assert.AreEqual("Test1", input.Col1);
//        //            Assert.AreEqual("Test2", input.B);
//        //        });
//        //    source.LinkTo(dest);
//        //    source.Execute();
//        //    dest.Wait();
//        //}


//        //[TestMethod]
//        //public void DBSourceWithSqlAndNoTableDefinition()
//        //{
//        //    SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Source
//        //        (Col1 nvarchar(100) null, Col2 nvarchar(100) null)");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");

//        //    SqlTask.ExecuteNonQuery("Create destination table", @"CREATE TABLE test.Destination
//        //        (Col1 nvarchar(100) null, Col2 nvarchar(100) null)");

//        //    DBSource source = new DBSource()
//        //    {
//        //        Sql = "SELECT Col1, Col2 FROM test.Source"
//        //    };
//        //    DBDestination dest = new DBDestination("test.Destination");
//        //    source.LinkTo(dest);
//        //    source.Execute();
//        //    dest.Wait();

//        //    Assert.AreEqual(3, RowCountTask.Count("test.Destination"));
//        //    Assert.AreEqual(3, RowCountTask.Count("test.Destination", "Col1 = 'Test1' AND Col2='Test2'"));
//        //}


//        //[TestMethod]
//        //public void DBSourceWithSqlAndNoTableDefinitionNotMatchingColumns()
//        //{
//        //    SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Source
//        //        (Col1 nvarchar(100) null, Col2 nvarchar(100) null)");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");

//        //    SqlTask.ExecuteNonQuery("Create destination table", @"CREATE TABLE test.Destination
//        //        (Col3 nvarchar(100) null, Col4 nvarchar(100) null, Col1 nvarchar(100) null)");

//        //    DBSource source = new DBSource()
//        //    {
//        //        Sql = "SELECT Col1, Col2 FROM test.Source"
//        //    };
//        //    DBDestination dest = new DBDestination("test.Destination");
//        //    source.LinkTo(dest);
//        //    source.Execute();
//        //    dest.Wait();

//        //    Assert.AreEqual(3, RowCountTask.Count("test.Destination"));
//        //    Assert.AreEqual(3, RowCountTask.Count("test.Destination", "Col3 = 'Test1' AND Col4='Test2'"));
//        //}


//        //[TestMethod]
//        //public void DBSourceWithSqlAndNoTableDefinitionLessColumns()
//        //{
//        //    SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Source
//        //        (Col1 nvarchar(100) null, Col2 nvarchar(100) null)");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");
//        //    SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1','Test2')");

//        //    SqlTask.ExecuteNonQuery("Create destination table", @"CREATE TABLE test.Destination
//        //        (Col nvarchar (100) not null )");

//        //    DBSource source = new DBSource()
//        //    {
//        //        Sql = "SELECT Col1, Col2 FROM test.Source"
//        //    };
//        //    DBDestination dest = new DBDestination("test.Destination");
//        //    source.LinkTo(dest);
//        //    source.Execute();
//        //    dest.Wait();

//        //    Assert.AreEqual(3, RowCountTask.Count("test.Destination"));
//        //    Assert.AreEqual(3, RowCountTask.Count("test.Destination", "Col = 'Test1'"));
//        //}
//    }

//}
