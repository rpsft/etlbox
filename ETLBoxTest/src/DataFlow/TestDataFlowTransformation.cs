using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestDataFlowTransformation {
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

        public class MySimpleRow {
            public string Col1 { get; set; }
            public int Col2 { get; set; }
        }

        /*
         * CSVSource (out: string[]) -> RowTransformation (in: string[], out: object)-> DBDestination (in: object)
         */
        [TestMethod]
        public void CSV_RowTrans_DB() {
            TableDefinition destinationTableDefinition = new TableDefinition("test.Staging", new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: true)
            });
            destinationTableDefinition.CreateTable();

            CSVSource source = new CSVSource("src/DataFlow/Simple_CSV2DB.csv");
            RowTransformation<string[], MySimpleRow> trans = new RowTransformation<string[], MySimpleRow>(
                csvdata => {
                    return new MySimpleRow() {
                        Col1 = csvdata[0],
                        Col2 = int.Parse(csvdata[1])
                    };
                });
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>() { DestinationTableDefinition = destinationTableDefinition };
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();
            Assert.AreEqual(3, RowCountTask.Count("test.Staging"));
        }

        /*
        * CSVSource (out: string[]) -> RowTransformation (in: string[], out: string[])-> DBDestination (in: string[])
        */
        [TestMethod]
        public void CSV_RowTrans_DB_NonGeneric() {
            SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Staging
                (Col1 int null, Col2 nvarchar(100) null)");

            CSVSource source = new CSVSource("src/DataFlow/Simple_CSV2DB.csv");
            RowTransformation trans = new RowTransformation(
                csvdata => {
                    return new string[] { csvdata[1], csvdata[0] };
                });
            DBDestination dest = new DBDestination("test.Staging");
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();
            Assert.AreEqual(3, RowCountTask.Count("test.Staging"));
        }


        /*
         * DBSource (out: object) -> RowTransformation (in: object, out: object) -> DBDestination (in: object)
         */
        [TestMethod]
        public void DB_RowTrans_DB() {
            TableDefinition sourceTableDefinition = CreateDBSourceTableForSimpleRow();
            TableDefinition destinationTableDefinition = CreateDBDestinationTableForSimpleRow();

            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>() { SourceTableDefinition = sourceTableDefinition };
            RowTransformation<MySimpleRow, MySimpleRow> trans = new RowTransformation<MySimpleRow, MySimpleRow>(myRow => {
                myRow.Col2 += 1;
                return myRow;
            });
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>() { DestinationTableDefinition = destinationTableDefinition };
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();
            Assert.AreEqual(3, SqlTask.ExecuteScalar<int>("Check destination table", "select count(*) from test.Destination"));
            Assert.AreEqual(9, SqlTask.ExecuteScalar<int>("Check destination table", "select sum(Col2) from test.Destination"));
        }

        internal TableDefinition CreateDBSourceTableForSimpleRow() {
            TableDefinition sourceTableDefinition = new TableDefinition("test.Source", new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: true)
            });
            sourceTableDefinition.CreateTable();
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1',1)");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test2',2)");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test3',3)");
            return sourceTableDefinition;
        }

        internal TableDefinition CreateDBDestinationTableForSimpleRow() {
            TableDefinition destinationTableDefinition = new TableDefinition("test.Destination", new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: true)
            });
            destinationTableDefinition.CreateTable();
            return destinationTableDefinition;
        }

        /*
         * DBSource (out: object) -> RowTransformation (in: object, out: object) --> DBDestination (in: object)
         */
        [TestMethod]
        public void DB_RowTrans_DB_WithInitAction() {
            TableDefinition sourceTableDefinition = CreateDBSourceTableForSimpleRow();
            TableDefinition destinationTableDefinition = CreateDBDestinationTableForSimpleRow();

            RowTransformationTestClass testClass = new RowTransformationTestClass();
            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>() { SourceTableDefinition = sourceTableDefinition };
            RowTransformation<MySimpleRow, MySimpleRow> trans = new RowTransformation<MySimpleRow, MySimpleRow>(
                "RowTransformation testing init Action",
                testClass.TestTransformationFunc,
                testClass.SetAddValue
            );
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>() { DestinationTableDefinition = destinationTableDefinition };
            source.LinkTo(trans);
            trans.LinkTo(dest);
            source.Execute();
            dest.Wait();
            Assert.AreEqual(3, SqlTask.ExecuteScalar<int>("Check destination table", "select count(*) from test.Destination"));
            Assert.AreEqual(9, SqlTask.ExecuteScalar<int>("Check destination table", "select sum(Col2) from test.Destination"));
        }

        public class RowTransformationTestClass {
            public int AddValue { get; set; } = 0;
            public void SetAddValue() {
                AddValue = 1;
            }

            public MySimpleRow TestTransformationFunc(MySimpleRow myRow) {
                myRow.Col2 += AddValue;
                return myRow;
            }
        }

        [TestMethod]
        public void TestLogging_DB_RowTrans_DB() {
            DataFlow.ClearSettings();
            CreateLogTablesTask.CreateLog();
            DB_RowTrans_DB();
            Assert.AreEqual(3, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='DF_DBSOURCE' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
            Assert.AreEqual(3, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='DF_DBDEST' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
        }

        [TestMethod]
        public void TestLogging_CSV_RowTrans_DB() {
            DataFlow.ClearSettings();
            CreateLogTablesTask.CreateLog();
            CSV_RowTrans_DB();
            Assert.AreEqual(3, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='DF_CSVSOURCE' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
            Assert.AreEqual(3, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='DF_DBDEST' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
        }
    }

}
