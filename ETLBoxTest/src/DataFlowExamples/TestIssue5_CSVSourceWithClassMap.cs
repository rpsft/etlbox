using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestIssue5_CSVSourceWithClassMap {
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

        public class CSVData
        {
            public string Col1 { get; set; }
            public int Col2 { get; set; }
        }

        public class ModelClassMap : ClassMap<CSVData>
        {
            public ModelClassMap()
            {
                Map(m => m.Col1).Name("Header1");
                Map(m => m.Col2).Name("Header2");
            }
        }

        [TestMethod]
        public void CSVGeneric_DB() {
            TableDefinition stagingTable = new TableDefinition("test.Staging", new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: true)
            });
            stagingTable.CreateTable();
            CSVSource<CSVData> source = new CSVSource<CSVData>("src/DataFlow/Simple_CSV2DB.csv");
            source.Configuration.RegisterClassMap<ModelClassMap>();
            DBDestination<CSVData> dest = new DBDestination<CSVData>() { DestinationTableDefinition = stagingTable };
            source.LinkTo(dest);

            source.Execute();
            dest.Wait();

            Assert.AreEqual(3, RowCountTask.Count("test.Staging","Col1 Like '%ValueRow%' and Col2 <> 1"));
        }
    }

}
