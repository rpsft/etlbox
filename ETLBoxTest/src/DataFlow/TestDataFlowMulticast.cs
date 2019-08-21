using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestDataFlowMulticast {
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

        public class MyDataRow {
            public string Col1 { get; set; }
            public int Col2 { get; set; }
        }

        /*
         * DBSource (out: object)
         * -> Multicast (in/out: object)
         * 1-> DBDestination (in: object) 2-> DBDestination (in: object)
         */
        [TestMethod]
        public void DB_Multicast_DB() {
            TableDefinition sourceTableDefinition = CreateTableForMyDataRow("test.Source");
            TableDefinition dest1TableDefinition = CreateTableForMyDataRow("test.Destination1");
            TableDefinition dest2TableDefinition = CreateTableForMyDataRow("test.Destination2");
            TableDefinition dest3TableDefinition = CreateTableForMyDataRow("test.Destination3");
            InsertDemoDataForMyRowTable("test.Source");

            DBSource<MyDataRow> source = new DBSource<MyDataRow>();
            source.SourceTableDefinition = sourceTableDefinition;
            Multicast<MyDataRow> multicast = new Multicast<MyDataRow>();
            DBDestination<MyDataRow> dest1 = new DBDestination<MyDataRow>();
            dest1.DestinationTableDefinition = dest1TableDefinition;
            DBDestination<MyDataRow> dest2 = new DBDestination<MyDataRow>();
            dest2.DestinationTableDefinition = dest2TableDefinition;
            DBDestination<MyDataRow> dest3 = new DBDestination<MyDataRow>();
            dest3.DestinationTableDefinition = dest3TableDefinition;

            source.LinkTo(multicast);
            multicast.LinkTo(dest1);
            multicast.LinkTo(dest2);
            multicast.LinkTo(dest3);
            source.Execute();
            dest1.Wait();
            dest2.Wait();
            dest3.Wait();

            Assert.AreEqual(3, RowCountTask.Count("test.Source","Col2 in (1,2,3)"));
            Assert.AreEqual(3, RowCountTask.Count("test.Destination1", "Col2 in (1,2,3)"));
            Assert.AreEqual(3, RowCountTask.Count("test.Destination2", "Col2 in (1,2,3)"));
            Assert.AreEqual(3, RowCountTask.Count("test.Destination3", "Col2 in (1,2,3)"));

        }

        internal TableDefinition CreateTableForMyDataRow(string tableName) {
            TableDefinition def = new TableDefinition(tableName, new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: true),
                new TableColumn("Col2", "int", allowNulls: true)
            });
            def.CreateTable();
            return def;
        }

        private static void InsertDemoDataForMyRowTable(string tableName) {
            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test1',1)");
            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test2',2)");
            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test3',3)");
        }


        /*
         *  DBSource (out: object)
         * -> Multicast (in/out: object)
         * 1-> DBDestination (in: object) 2-> DBDestination (in: object)
         */
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

            var source = new CSVSource<TestPoco>("src/DataFlowExamples/Issue5.csv")
            {
                Configuration = new CsvHelper.Configuration.Configuration() { Delimiter = ";" }
            };
            var multicast = new Multicast<TestPoco>();
            var destination1 = new DBDestination<TestEntity1>("test.Table1");
            var destination2 = new DBDestination<TestEntity2>("test.Table2");

            source.LinkTo(multicast);
            multicast.LinkTo(row1);
            multicast.LinkTo(row2);

            row1.LinkTo(destination1);
            row2.LinkTo(destination2);

            source.Execute();
            destination1.Wait();
            destination2.Wait();

            Assert.AreEqual(2, RowCountTask.Count("test.Table1", "Col1 in ('one','five') and Col3 in ('three','seven')"));
            Assert.AreEqual(2, RowCountTask.Count("test.Table2", "Col2 in ('two','six') and Col4 in ('four','eight')"));
        }

        /*
         * DBSource (out: string[])
         * -> Multicast (in/out: string[])
         * 1-> DBDestination (in: string[]) 2-> DBDestination (in: string[])
         */
        [TestMethod]
        public void DB_Multicast_DB_WithStringArray()
        {
            TableDefinition sourceTableDefinition = CreateTableForMyDataRow("test.Source");
            TableDefinition dest1TableDefinition = CreateTableForMyDataRow("test.Destination1");
            TableDefinition dest2TableDefinition = CreateTableForMyDataRow("test.Destination2");
            InsertDemoDataForMyRowTable("test.Source");

            DBSource source = new DBSource();
            source.SourceTableDefinition = sourceTableDefinition;
            Multicast multicast = new Multicast();
            DBDestination dest1 = new DBDestination();
            dest1.DestinationTableDefinition = dest1TableDefinition;
            DBDestination dest2 = new DBDestination();
            dest2.DestinationTableDefinition = dest2TableDefinition;

            source.LinkTo(multicast);
            multicast.LinkTo(dest1);
            multicast.LinkTo(dest2);
            source.Execute();
            dest1.Wait();
            dest2.Wait();

            Assert.AreEqual(3, RowCountTask.Count("test.Source", "Col2 in (1,2,3)"));
            Assert.AreEqual(3, RowCountTask.Count("test.Destination1", "Col2 in (1,2,3)"));
            Assert.AreEqual(3, RowCountTask.Count("test.Destination2", "Col2 in (1,2,3)"));

        }
    }

}
