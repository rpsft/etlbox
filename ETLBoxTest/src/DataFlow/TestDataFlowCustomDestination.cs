//using ALE.ETLBox;
//using ALE.ETLBox.ConnectionManager;
//using ALE.ETLBox.ControlFlow;
//using ALE.ETLBox.DataFlow;
//using ALE.ETLBox.Logging;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using Newtonsoft.Json;
//using System;
//using System.Collections.Generic;
//using System.IO;


//namespace ALE.ETLBoxTest {
//    [TestClass]
//    public class TestDataFlowCustomDestination {
//        public TestContext TestContext { get; set; }
//        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
//        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

//        [ClassInitialize]
//        public static void ClassInit(TestContext testContext) {
//            TestHelper.RecreateDatabase(testContext);
//            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
//            CreateSchemaTask.Create("test");
//        }

//        [TestInitialize]
//        public void TestInit() {
//            CleanUpSchemaTask.CleanUp("test");
//        }

//        public class MySimpleRow {
//            public string Col1 { get; set; }
//            public int Col2 { get; set; }
//        }

//        /*
//         * DSBSource (out: object) -> CustomDestination (in: object)
//         */
//        [TestMethod]
//        public void DB_CustDest() {
//            TableDefinition sourceTableDefinition = CreateSourceTable("test.Source");
//            TableDefinition destinationTableDefinition = CreateDestinationTable("test.Destination");

//            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>() { SourceTableDefinition = sourceTableDefinition };
//            CustomDestination<MySimpleRow> dest = new CustomDestination<MySimpleRow>(
//                row => {
//                    SqlTask.ExecuteNonQuery("Insert row", $"insert into test.Destination values('{row.Col1}',{row.Col2})");
//                    }
//            );
//            source.LinkTo(dest);
//            source.Execute();
//            dest.Wait();
//            Assert.AreEqual(3, RowCountTask.Count("test.Destination","Col2 > 0"));
//        }

//        [TestMethod]
//        public void Table2JsonFile()
//        {
//            CreateLogTablesTask.CreateLog();
//            DataFlow.LoggingThresholdRows = 2;
//            TableDefinition sourceTableDefinition = CreateSourceTable("test.Source");

//            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(sourceTableDefinition);

//            List<MySimpleRow> rows = new List<MySimpleRow>();
//            CustomDestination<MySimpleRow> dest = new CustomDestination<MySimpleRow>(
//                row => {
//                    rows.Add(row);
//                }
//            );

//            source.LinkTo(dest);
//            source.Execute();
//            dest.Wait();
//            string json = JsonConvert.SerializeObject(rows, Formatting.Indented);

//            Assert.AreEqual(json, File.ReadAllText("src/DataFlow/json_tobe.json"));
//        }

//        private static TableDefinition CreateSourceTable(string tableName) {
//            TableDefinition sourceTableDefinition = new TableDefinition(tableName, new List<TableColumn>() {
//                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
//                new TableColumn("Col2", "int", allowNulls: true)
//            });
//            sourceTableDefinition.CreateTable();
//            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test1',1)");
//            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test2',2)");
//            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test3',3)");
//            return sourceTableDefinition;
//        }

//        private static TableDefinition CreateDestinationTable(string tableName) {
//            TableDefinition destinationTableDefinition = new TableDefinition(tableName, new List<TableColumn>() {
//                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
//                new TableColumn("Col2", "int", allowNulls: true)
//            });
//            destinationTableDefinition.CreateTable();
//            return destinationTableDefinition;
//        }



//    }

//}
