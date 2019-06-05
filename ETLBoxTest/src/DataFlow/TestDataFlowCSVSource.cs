using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using CsvHelper.Configuration.Attributes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestDataFlowCSVSource {
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

        /*
         * CSVSource (out: string[]) -> DBDestination (in: string[])
         * Table without key columns
         */
        [TestMethod]
        public void CSV_DB() {
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: true)
            };
            Load_CSV_DB(columns);
        }

        /*
         * CSVSource (out: string[]) -> DBDestination (in: string[])
         * Table without key columns, number of columns do not match
         * (there are more columns in Datbase than in CSV file)
         */
        [TestMethod]
        public void CSV_DB_MoreColumnsInDB() {
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: false),
                new TableColumn("Col3", "nvarchar(100)", allowNulls: true)
            };
            Load_CSV_DB(columns);
        }

        /*
         * CSVSource (out: string[]) -> DBDestination (in: string[])
         * Table without key columns, number of columns do not match
         * (there are more columns in CSV than in Database)
         */
        [TestMethod]
        public void CSV_DB_MoreColumnsInCSV() {
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false)
            };
            Load_CSV_DB(columns);
        }

        private static void Load_CSV_DB(List<TableColumn> columnsInStageTable) {
            TableDefinition stagingTable = new TableDefinition("test.Staging", columnsInStageTable);
            stagingTable.CreateTable();
            CSVSource source = new CSVSource("src/DataFlow/Simple_CSV2DB.csv");
            DBDestination<string[]> dest = new DBDestination<string[]>() { DestinationTableDefinition = stagingTable };
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
            Assert.AreEqual(3, RowCountTask.Count("test.Staging", "Col1 Like '%ValueRow%'"));
        }

        /*
         * CSVSource (out: string[]) -> DBDestination (in: string[])
         * Table with key column (at different positions)
         */
        [TestMethod]
        public void CSV_DB_WithKeyPosition1() {
            CSV_DB_WithKey(0);
        }

        [TestMethod]
        public void CSV_DB_WithKeyPosition2() {
            CSV_DB_WithKey(1);
        }

        [TestMethod]
        public void CSV_DB_WithKeyPosition3() {
            CSV_DB_WithKey(2);
        }

        public void CSV_DB_WithKey(int keyPosition) {
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: true) };
            columns.Insert(keyPosition, new TableColumn("Key", "int", allowNulls: false, isPrimaryKey: true) { IsIdentity = true });
            TableDefinition stagingTable = new TableDefinition($"test.Staging{keyPosition}", columns);
            stagingTable.CreateTable();
            CSVSource source = new CSVSource("src/DataFlow/Simple_CSV2DB.csv");
            DBDestination<string[]> dest = new DBDestination<string[]>() { DestinationTableDefinition = stagingTable };
            source.LinkTo(dest);

            source.Execute();
            dest.Wait();

            Assert.AreEqual(3, RowCountTask.Count($"test.Staging{keyPosition}","Col1 Like '%ValueRow%' and Col2 <> 1"));
        }

        /*
         * CSVSource (out: string[]) -> DBDestination (in: string[])
         */
        [TestMethod]
        public void CSV_DB_WithBatchChanges() {
            TableDefinition stagingTable = new TableDefinition("test.Staging", new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: true)
            });
            stagingTable.CreateTable();
            CSVSource source = new CSVSource("src/DataFlow/Simple_CSV2DB.csv");
            DBDestination<string[]> dest = new DBDestination<string[]>(batchSize: 2) {
                DestinationTableDefinition = stagingTable,
                BeforeBatchWrite =
                rowArray => {
                    rowArray[0][0] = "NewValue";
                    return rowArray;
                }
            };
            source.LinkTo(dest);

            source.Execute();
            dest.Wait();

            Assert.AreEqual(1, RowCountTask.Count("test.Staging","Col1 Like '%ValueRow%' and Col2 <> 1"));
            Assert.AreEqual(2, RowCountTask.Count("test.Staging","Col1 = 'NewValue'"));
        }

        public class CSVData {
            [Name("Header1")]
            public string Col1 { get; set; }
            [Name("Header2")]
            public int Col2 { get; set; }
        }
        [TestMethod]
        public void CSVGeneric_DB() {
            TableDefinition stagingTable = new TableDefinition("test.Staging", new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: true)
            });
            stagingTable.CreateTable();
            CSVSource<CSVData> source = new CSVSource<CSVData>("src/DataFlow/Simple_CSV2DB.csv");
            DBDestination<CSVData> dest = new DBDestination<CSVData>() { DestinationTableDefinition = stagingTable };
            source.LinkTo(dest);

            source.Execute();
            dest.Wait();

            Assert.AreEqual(3, RowCountTask.Count("test.Staging","Col1 Like '%ValueRow%' and Col2 <> 1"));
        }

        [TestMethod]
        public void CSVGenericWithSkipRows_DB()
        {
            TableDefinition stagingTable = new TableDefinition("test.Staging", new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: true)
            });
            stagingTable.CreateTable();
            CSVSource<CSVData> source = new CSVSource<CSVData>("src/DataFlow/CSVSkipRows.csv");
            source.SkipRows = 2;
            DBDestination<CSVData> dest = new DBDestination<CSVData>() { DestinationTableDefinition = stagingTable };
            source.LinkTo(dest);

            source.Execute();
            dest.Wait();

            Assert.AreEqual(3, RowCountTask.Count("test.Staging", "Col1 Like '%ValueRow%' and Col2 <> 1"));
        }
    }

}
