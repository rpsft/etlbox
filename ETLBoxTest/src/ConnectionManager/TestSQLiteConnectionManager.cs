using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace ALE.ETLBoxTest
{
    [TestClass]
    public class TestSQLiteConnectionManager
    {
        public TestContext TestContext { get; set; }

        public string SQLiteConnectionStringParameter => TestContext?.Properties["sqliteConnectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext)
        {
            var cs = new ConnectionString(testContext?.Properties["connectionString"].ToString());
            var masterConnection = new SqlConnectionManager(cs.GetMasterConnection());
            ControlFlow.SetLoggingDatabase(masterConnection);

            SQLiteConnectionManager con = new SQLiteConnectionManager(new SQLiteConnectionString(testContext?.Properties["sqliteConnectionString"].ToString()));
            ControlFlow.CurrentDbConnection = con;
        }

        [TestMethod]
        public void TestSqlTaskWithSQLiteConnection()
        {

            new SqlTask($"Test statement", $@"
                    CREATE TABLE test (
                        Col1 nvarchar(50)
                    );

                    INSERT INTO test  (Col1)
                    VALUES('Lorem ipsum Lorem ipsum Lorem ipsum Lorem'); ")
            {
                DisableLogging = true
            }.ExecuteNonQuery();

            Assert.AreEqual(RowCountTask.Count("test").Value, 1);
        }


        [TestMethod]
        public void TestDataflowWithSQLite()
        {
            //ControlFlow.CurrentDbConnection = new SQLiteConnectionManager(new SQLiteConnectionString(SQLiteConnectionStringParameter));

            new SqlTask($"Test statement", $@"
                    CREATE TABLE source (
                        ID INTEGER PRIMARY KEY,
                        Col1 TEXT NOT NULL,
                        Col2 INTEGER NOT NULL
                    );
                    CREATE TABLE dest (
                        ID INTEGER PRIMARY KEY,
                        Col1 TEXT NOT NULL,
                        Col2 INTEGER NOT NULL
                    );
                    INSERT INTO source  (Col1, Col2)
                    VALUES('Value1',1),
                          ('Value2',2); ")
            {
                DisableLogging = true
            }.ExecuteNonQuery();

            var tableDefinition = new TableDefinition("source",
                new List<TableColumn>() { new TableColumn("Col1", "TEXT"),
                    new TableColumn("Col2", "INTEGER") });
            DBSource source = new DBSource("source")
            {
                SourceTableDefinition = tableDefinition
            };
            DBDestination dest = new DBDestination("dest")
            {
                DestinationTableDefinition = tableDefinition
            };
            source.LinkTo(dest);

            source.Execute();
            dest.Wait();

            Assert.AreEqual(2, RowCountTask.Count("dest").Value);
        }


        [TestMethod]
        [ExpectedException(typeof(ETLBoxException))]
        public void TestCreateSchemaTask()
        {
            CreateSchemaTask.Create("test");
        }


    }
}
