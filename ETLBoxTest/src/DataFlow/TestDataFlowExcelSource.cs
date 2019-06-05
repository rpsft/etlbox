using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestDataFlowExcelSource {
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

        public class ExcelData {
            [ExcelColumn(1)]
            public string Col2 { get; set; }
            [ExcelColumn(2)]
            public decimal Col3 { get; set; }
            public string Empty { get; set; } = "";
            [ExcelColumn(0)]
            public int Col1 { get; set; }
        }


        [TestMethod]
        public void Excel_DB() {
            TableDefinition stagingTable = new TableDefinition("test.Staging", new List<TableColumn>() {
                new TableColumn("Col1", "int", allowNulls: false),
                new TableColumn("Col2", "nvarchar(100)", allowNulls: true),
                new TableColumn("Col3", "decimal(10,2)", allowNulls: true)
            });
            stagingTable.CreateTable();
            ExcelSource<ExcelData> source = new ExcelSource<ExcelData>("src/DataFlow/ExcelDataFile.xlsx") {
                Range = new ExcelRange(2, 4, 5, 9),
                SheetName = "Sheet2"
            };
            DBDestination<ExcelData> dest = new DBDestination<ExcelData>() { DestinationTableDefinition = stagingTable };
            source.LinkTo(dest);

            source.Execute();
            dest.Wait();

            Assert.AreEqual(5, RowCountTask.Count("test.Staging"));
        }

    }

}
