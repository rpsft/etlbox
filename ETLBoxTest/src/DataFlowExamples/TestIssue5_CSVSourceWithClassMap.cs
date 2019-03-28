using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using CsvHelper.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace ALE.ETLBoxTest
{
    [TestClass]
    public class TestIssue5_CSVSourceWithClassMap
    {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void ClassInit(TestContext testContext)
        {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateSchemaTask.Create("test");
        }

        [TestInitialize]
        public void TestInit()
        {
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
        public void CSVGeneric_DB()
        {
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

            Assert.AreEqual(3, RowCountTask.Count("test.Staging", "Col1 Like '%ValueRow%' and Col2 <> 1"));
        }


        internal class TestPoco
        {
            public string Value1 { get; set; }
            public string Value2 { get; set; }
            public string Value3 { get; set; }
            public string Value4 { get; set; }
        }

        internal class TestEntity1
        {
            public string Col1 { get; set; }
            public string Col3 { get; set; }
        }

        internal class TestEntity2
        {
            public string Col2 { get; set; }
            public string Col4 { get; set; }
        }

        private TableDefinition CreateTable(string tablename)
        {
            var def = new TableDefinition(tablename, new List<TableColumn>
          {
            new TableColumn("Col1", "nvarchar(100)", true),
            new TableColumn("Col2", "nvarchar(100)", true),
            new TableColumn("Col3", "nvarchar(100)", true),
            new TableColumn("Col4", "nvarchar(100)", true)
          });
            def.CreateTable();
            return def;
        }

        [TestMethod]
        public void Multicast_Into2Tables()
        {
            var tableDestination1 = this.CreateTable("test.Table1");
            var tableDestination2 = this.CreateTable("test.Table2");

            var row1 = new RowTransformation<TestPoco, TestEntity1>(input => {
                return new TestEntity1
                {
                    Col1 = input.Value1,
                    Col3 = input.Value3
                };
            });
            var row2 = new RowTransformation<TestPoco, TestEntity2>(input => {
                return new TestEntity2
                {
                    Col2 = input.Value2,
                    Col4 = input.Value4
                };
            });

            var source = new CSVSource<TestPoco>("src/DataFlowExamples/Issue5.csv") {
                Configuration = new CsvHelper.Configuration.Configuration() { Delimiter = ";" }
            };
            var multicast = new Multicast<TestPoco>();
            var destination1 = new DBDestination<TestEntity1>("test.Table1");
            var destination2 = new DBDestination<TestEntity2>("test.Table2");

            source.LinkTo(multicast);
            multicast.LinkTo(row1);
            //multicast.LinkTo(row2);

            row1.LinkTo(destination1);
            //row2.LinkTo(destination2);

            source.Execute();
            destination1.Wait();
           // destination2.Wait();
        }
    }
}


