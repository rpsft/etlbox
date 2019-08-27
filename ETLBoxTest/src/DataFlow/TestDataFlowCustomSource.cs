//using ALE.ETLBox;
//using ALE.ETLBox.ConnectionManager;
//using ALE.ETLBox.ControlFlow;
//using ALE.ETLBox.DataFlow;
//using ALE.ETLBox.Logging;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using System.Collections.Generic;

//namespace ALE.ETLBoxTest {
//    [TestClass]
//    public class TestDataFlowCustomSource {
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

//        public class CustomRowReader {
//            public List<string> Data { get; set; } = new List<string>() { "Test1", "Test2", "Test3" };
//            public int _readIndex = 0;
//            public MySimpleRow ReadData() {
//                var result = new MySimpleRow() {
//                    Col1 = Data[_readIndex],
//                    Col2 = _readIndex
//                };
//                _readIndex++;
//                return result;
//            }

//            public bool EndOfData() {
//                return _readIndex >= Data.Count;
//            }
//        }

//        /*
//         * CustomSource (out: object) -> DBDestination (in: object)
//         */
//        [TestMethod]
//        public void CustSource_DB() {
//            TableDefinition destinationTableDefinition = CreateDestinationTable("test.Destination");

//            CustomRowReader rowReaderClass = new CustomRowReader();
//            CustomSource<MySimpleRow> source = new CustomSource<MySimpleRow>(rowReaderClass.ReadData, rowReaderClass.EndOfData);
//            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(destinationTableDefinition);
//            source.LinkTo(dest);
//            source.Execute();
//            dest.Wait();
//            Assert.AreEqual(3, RowCountTask.Count("test.Destination"));
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
