using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestAccessConnectionViaOdbc {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string OdbcConnectionStringParameter => TestContext?.Properties["odbcConnectionString"].ToString();
        public string AccessConnectionStringParameter => TestContext?.Properties["accessConnectionString"].ToString();

        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
        }

        //Download and configure Odbc driver for access first! This test points to access file on local path
        //Odbc driver needs to be 64bit! 
        //https://www.microsoft.com/en-us/download/details.aspx?id=13255
        [TestMethod]
        public void CSV2ACCESS_ViaOdbc() {
            ControlFlow.CurrentDbConnection = new AccessOdbcConnectionManager(new OdbcConnectionString(AccessConnectionStringParameter)) {
                AlwaysUseSameConnection = false
            };
            TableDefinition testTable = RecreateTestTable();


            CSVSource source = new CSVSource("src/ConnectionManager/AccessData.csv");
            DBDestination<string[]> dest = new DBDestination<string[]>(testTable, 2);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
            Assert.AreEqual(6, RowCountTask.Count(testTable.Name));
        }

        private static TableDefinition RecreateTestTable() {
            try {
                SqlTask.ExecuteNonQuery("Try to drop table", @"DROP TABLE TestTable;");
            } catch { }
            TableDefinition testTable = new TableDefinition("TestTable", new List<TableColumn>() {
                new TableColumn("Field1", "NUMBER", allowNulls: true),
                new TableColumn("Field2", "CHAR", allowNulls: true)
            });
            new CreateTableTask(testTable) { ThrowErrorIfTableExists = true }.Execute();
            return testTable;
        }

        [TestMethod]
        public void BigData_CSV_ACCESS_ViaOdbc() {
            int numberOfRows = 2000;
            ControlFlow.CurrentDbConnection = new AccessOdbcConnectionManager(new OdbcConnectionString(AccessConnectionStringParameter)) {
                AlwaysUseSameConnection = false
            };
            Stopwatch watch = new Stopwatch();
            TableDefinition stagingTable = new TableDefinition("staging", new List<TableColumn>() {
                new TableColumn("Col1", "CHAR", allowNulls: true),
                new TableColumn("Col2", "CHAR", allowNulls: true),
                new TableColumn("Col3", "CHAR", allowNulls: false),
                new TableColumn("Col4", "CHAR", allowNulls: false),
            });
            try {
                SqlTask.ExecuteNonQuery("Try to drop table", $@"DROP TABLE {stagingTable.Name};");
            } catch { }
            new CreateTableTask(stagingTable) { ThrowErrorIfTableExists = true }.Execute();
            string fileName = "src/ConnectionManager/AccessBigData_CSV2DB.csv";
            BigDataHelper bigData = new BigDataHelper() {
                FileName = fileName,
                NumberOfRows = numberOfRows,
                TableDefinition = stagingTable
            };
            watch.Start();
            LogTask.Info($"Create .csv file {fileName} with {numberOfRows} Rows");
            bigData.CreateBigDataCSV();
            LogTask.Info($"Needed {watch.Elapsed.TotalMinutes} to create .csv file");
            watch.Reset();

            CSVSource source = new CSVSource(fileName);
            DBDestination<string[]> dest = new DBDestination<string[]>(30) { DestinationTableDefinition = stagingTable };
            source.LinkTo(dest);
            watch.Start();
            source.Execute();
            dest.Wait();
            LogTask.Info($"Needed {watch.Elapsed.TotalMinutes} to write everything into database");

            Assert.AreEqual(numberOfRows, RowCountTask.Count(stagingTable.Name));
        }

        [TestMethod]
        public void ACCESS2CSV_ViaOdbc() {
            ControlFlow.CurrentDbConnection = new AccessOdbcConnectionManager(new OdbcConnectionString(AccessConnectionStringParameter)) {
                AlwaysUseSameConnection = false
            };
            var sqlConnMan = new SqlConnectionManager(new ConnectionString(ConnectionStringParameter));
            TableDefinition testTable = RecreateTestTable();
            SqlTask.ExecuteNonQuery("Insert test data", "INSERT INTO TestTable (Field1, Field2) values (1,'Test1');");
            SqlTask.ExecuteNonQuery("Insert test data", "INSERT INTO TestTable (Field1, Field2) values (2,'Test2');");
            SqlTask.ExecuteNonQuery("Insert test data", "INSERT INTO TestTable (Field1, Field2) values (3,'Test3');");

            new SqlTask("Create Target Table", $@"CREATE TABLE dbo.TargetTable (
    Field1 decimal not null, Field2 nvarchar(1000) not null)") {
                ConnectionManager = sqlConnMan
            }
                .ExecuteNonQuery();
            DBSource<Data> source = new DBSource<Data>(testTable);
            DBDestination<Data> dest = new DBDestination<Data>("dbo.TargetTable", 1) {
                ConnectionManager = sqlConnMan
            };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
            Assert.AreEqual(3, (new RowCountTask("dbo.TargetTable") {
                ConnectionManager = sqlConnMan
            }).Count().Rows);
        }

        public class Data {
            public Double Field1 { get; set; }
            public string Field2 { get; set; }
        }

    }
}
