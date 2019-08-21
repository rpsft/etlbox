using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace ALE.ETLBoxTest
{
    [TestClass]
    public class TestDataFlowDifferentDBs
    {
        public TestContext TestContext { get; set; }
        public string ConnectionStringSource => TestContext?.Properties["connectionString"].ToString();
        public string DBNameSource => TestContext?.Properties["dbName"].ToString();
        public string ConnectionStringDest => TestContext?.Properties["connectionStringDest"].ToString();
        public string DBNameDest => TestContext?.Properties["dbNameDest"].ToString();
        public string ConnectionStringLog => TestContext?.Properties["connectionStringLog"].ToString();
        public string DBNameLog => TestContext?.Properties["dbNameLog"].ToString();

        [ClassInitialize]
        public static void ClassInit(TestContext testContext)
        {
            TestHelper.RecreateDatabase(testContext);
            TestHelper.RecreateDatabase(testContext.Properties["dbNameDest"].ToString(), testContext.Properties["connectionStringDest"].ToString());
            TestHelper.RecreateDatabase(testContext.Properties["dbNameLog"].ToString(), testContext.Properties["connectionStringLog"].ToString());

            new CreateSchemaTask("test")
            {
                ConnectionManager = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()))
            }.Execute();
            new CreateSchemaTask("test")
            {
                ConnectionManager = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionStringDest"].ToString()))
            }.Execute();
        }

        [TestInitialize]
        public void TestInit()
        {
            new CleanUpSchemaTask("test")
            {
                ConnectionManager = new SqlConnectionManager(new ConnectionString(ConnectionStringSource))
            }.Execute() ;
            new CleanUpSchemaTask("test")
            {
                ConnectionManager = new SqlConnectionManager(new ConnectionString(ConnectionStringDest))
            }.Execute();
        }

        [TestMethod]
        public void TestTransferBetweenDBs()
        {
            var sourceConnection = new SqlConnectionManager(new ConnectionString(ConnectionStringSource));
            var destConnection = new SqlConnectionManager(new ConnectionString(ConnectionStringDest));

            ControlFlow.CurrentDbConnection = new SqlConnectionManager(ConnectionStringSource);
            SqlTask.ExecuteNonQuery("Drop source table", @"DROP TABLE IF EXISTS test.Source");
            SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Source
                (Col1 nvarchar(100) null, Col2 int null)");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1',1)");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test2',2)");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test3',3)");

            ControlFlow.CurrentDbConnection = new SqlConnectionManager(ConnectionStringDest);
            SqlTask.ExecuteNonQuery("Drop source table", @"DROP TABLE IF EXISTS test.Destination");
            SqlTask.ExecuteNonQuery("Create destination table", @"CREATE TABLE test.Destination
                (Col1 nvarchar(30) null, Col2 bigint null)");

            DBSource source = new DBSource(sourceConnection, "test.Source");
            DBDestination dest = new DBDestination(destConnection, "test.Destination");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            Assert.AreEqual(3, RowCountTask.Count("test.Destination"));
            Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col1 = 'Test1' AND Col2=1"));
            Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col1 = 'Test2' AND Col2=2"));
            Assert.AreEqual(1, RowCountTask.Count("test.Destination", "Col1 = 'Test3' AND Col2=3"));
        }

        [TestMethod]
        public void TestTransferAndLogging()
        {
            //CurrentDbConnection is always use if ConnectionManager is not specified otherwise!
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(ConnectionStringSource);

            SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Source
                            (Col1 nvarchar(100) null, Col2 int null)");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1',1)");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test2',2)");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test3',3)");

            ControlFlow.CurrentDbConnection = new SqlConnectionManager(ConnectionStringLog);
            CreateLogTablesTask.CreateLog();

            new CreateTableTask("test.Destination", new List<ITableColumn>() {
                            new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                            new TableColumn("Col2", "int", allowNulls: true)
                        })
            {
                ConnectionManager = new SqlConnectionManager(ConnectionStringDest)
            }.Execute();


            var sourceConnection = new SqlConnectionManager(new ConnectionString(ConnectionStringSource));
            var destConnection = new SqlConnectionManager(new ConnectionString(ConnectionStringDest));

            DBSource source = new DBSource(sourceConnection, "test.Source");
            RowTransformation trans = new RowTransformation(row =>
            {
                LogTask.Info($"Test message: {row[0]}, {row[1]}"); //Log DB is used as this is the ControlFlow.CurrentDBConnection!
                return row;
            });

            DBDestination destination = new DBDestination(destConnection, "test.Destination");

            source.LinkTo(trans);
            trans.LinkTo(destination);
            source.Execute();
            destination.Wait();

            Assert.AreEqual(1, new RowCountTask("test.Destination", "Col1 = 'Test1' AND Col2=1")
            {
                ConnectionManager = new SqlConnectionManager(ConnectionStringDest)
            }.Count().Rows);
            Assert.AreEqual(1, new RowCountTask("test.Destination", "Col1 = 'Test2' AND Col2=2")
            {
                ConnectionManager = new SqlConnectionManager(ConnectionStringDest)
            }.Count().Rows);
            Assert.AreEqual(1, new RowCountTask("test.Destination", "Col1 = 'Test3' AND Col2=3")
            {
                ConnectionManager = new SqlConnectionManager(ConnectionStringDest)
            }.Count().Rows);
        } 

    }

}
