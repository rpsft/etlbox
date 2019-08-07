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

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestSQLiteConnectionManager {
        public TestContext TestContext { get; set; }

        public string SQLiteConnectionStringParameter => TestContext?.Properties["sqliteConnectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            var cs = new ConnectionString(testContext?.Properties["connectionString"].ToString());
            var masterConnection = new SqlConnectionManager(cs.GetMasterConnection());
            ControlFlow.SetLoggingDatabase(masterConnection);

            SQLiteConnectionManager con = new SQLiteConnectionManager(new SQLiteConnectionString(testContext?.Properties["sqliteConnectionString"].ToString()));
            ControlFlow.CurrentDbConnection = con;
        }

        [TestMethod]
        public void TestSqlTaskWithSQLiteConnection() {

            new SqlTask($"Test statement", $@"
                    CREATE TABLE test (
                        Col1 nvarchar(50)
                    );

                    INSERT INTO test  (Col1)
                    VALUES('Lorem ipsum Lorem ipsum Lorem ipsum Lorem'); ") {
                DisableLogging = true
            }.ExecuteNonQuery();

            Assert.AreEqual(RowCountTask.Count("test").Value, 1);
        }

        /*
        [TestMethod]
        public void CSV_DB_WithOdbcConnection() {
            ControlFlow.CurrentDbConnection = new OdbcConnectionManager(new OdbcConnectionString(OdbcConnectionStringParameter));

            CreateSchemaTask.Create("test");
            TableDefinition stagingTable = new TableDefinition("test.Staging", new List<TableColumn>() {
                new TableColumn("ID", "int", allowNulls: false,isPrimaryKey:true,isIdentity:true),
                new TableColumn("Col1", "bit", allowNulls: true),
                new TableColumn("Col2", "decimal(10,5)", allowNulls: true),
                new TableColumn("Col3", "tinyint", allowNulls: true),
                new TableColumn("Col4", "int", allowNulls: true),
                new TableColumn("Col5", "uniqueidentifier", allowNulls: true),
                new TableColumn("Col6", "nvarchar(100)", allowNulls: true)
            });
            stagingTable.CreateTable();
            CSVSource source = new CSVSource("src/ConnectionManager/DatatypeCSV.csv")
            {
                Configuration = new CsvHelper.Configuration.Configuration() { Quote = '"' }
            };
            RowTransformation<string[], string[]> trans = new RowTransformation<string[], string[]>("Set empty values to null",
                row => {
                    for (int i=0;i<row.Length;i++)
                        if (row[i] == String.Empty) row[i] = null;
                    return row;
                });
            DBDestination<string[]> dest = new DBDestination<string[]>(stagingTable, 2);
            source.LinkTo(trans);
            trans.LinkTo(dest);

            source.Execute();
            dest.Wait();


            Assert.AreEqual(3, RowCountTask.Count(stagingTable.Name));
        }

        [TestMethod]
        [Ignore("nlog does not support odbc connections within .net core")]
        public void TestLoggingWithOdbc() {
            ControlFlow.CurrentDbConnection = new OdbcConnectionManager(new OdbcConnectionString(OdbcConnectionStringParameter));
            CreateLogTablesTask.CreateLog();
            LogTask.Info("Info");
            Assert.AreEqual(1, SqlTask.ExecuteScalar<int>("Check if default log works", "select count(*) from etl.Log where Message in ('Error','Warn','Info')"));
        }

     */
    }
}
