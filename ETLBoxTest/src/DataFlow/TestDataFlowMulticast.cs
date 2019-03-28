using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
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
            InsertDemoDataForMyRowTable("test.Source");

            DBSource<MyDataRow> source = new DBSource<MyDataRow>();
            source.SourceTableDefinition = sourceTableDefinition;
            Multicast<MyDataRow> multicast = new Multicast<MyDataRow>();
            DBDestination<MyDataRow> dest1 = new DBDestination<MyDataRow>();
            dest1.DestinationTableDefinition = dest1TableDefinition;
            DBDestination<MyDataRow> dest2 = new DBDestination<MyDataRow>();
            dest2.DestinationTableDefinition = dest2TableDefinition;

            source.LinkTo(multicast);
            multicast.LinkTo(dest1);
            multicast.LinkTo(dest2);
            source.Execute();
            dest1.Wait();
            dest2.Wait();

            Assert.AreEqual(3, RowCountTask.Count("test.Source","Col2 in (1,2,3)"));
            Assert.AreEqual(3, RowCountTask.Count("test.Destination1", "Col2 in (1,2,3)"));
            Assert.AreEqual(3, RowCountTask.Count("test.Destination2", "Col2 in (1,2,3)"));

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
    }

}
