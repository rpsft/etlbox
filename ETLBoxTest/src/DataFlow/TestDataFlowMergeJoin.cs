using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestDataFlowMergeJoin {
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

        public class MyDataRow1 {
            public string Col1 { get; set; }
            public int Col2 { get; set; }
        }

        public class MyDataRow2 {
            public int Col3 { get; set; }                
        }


        /*
         * 1-DBSource (out: MyDataRow1) 
         * 2-DBSource (out: MyDataRow2) 
         * 1,2-> Join (in: MyDataRow1, in: MyDataRow2, out: MyDataRow1) 
         * -> DBDestination (in: MyDataRow1)
         */
        [TestMethod]
        public void DB_MergeJoin_DB() {
            TableDefinition source1TableDefinition = CreateTableForInput1("test.Source1");
            TableDefinition source2TableDefinition = CreateTableForInput2("test.Source2");
            TableDefinition destTableDefinition = CreateTableForDestination("test.Destination");

            DBSource<MyDataRow1> source1 = new DBSource<MyDataRow1>() { SourceTableDefinition = source1TableDefinition };
            DBSource<MyDataRow2> source2 = new DBSource<MyDataRow2>() { SourceTableDefinition = source2TableDefinition };

            MergeJoin<MyDataRow1, MyDataRow2, MyDataRow1> join = new MergeJoin<MyDataRow1, MyDataRow2, MyDataRow1>(
                (input1, input2) => {
                    input1.Col2 += input2.Col3;
                    return input1;
                });

            DBDestination<MyDataRow1> dest = new DBDestination<MyDataRow1>() { DestinationTableDefinition = destTableDefinition };
            source1.LinkTo(join.Target1);
            source2.LinkTo(join.Target2);
            join.LinkTo(dest);

            source1.Execute();
            source2.Execute();
            dest.Wait();            

            Assert.AreEqual(3, RowCountTask.Count("test.Destination","Col2 in (11,102,1003)"));
            

        }

        internal TableDefinition CreateTableForInput1(string tableName) {
            TableDefinition def = new TableDefinition(tableName, new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: true),
                new TableColumn("Col2", "int", allowNulls: true)
            });
            def.CreateTable();
            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test1',1)");
            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test2',2)");
            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test3',3)");
            
            return def;
        }

        internal TableDefinition CreateTableForInput2(string tableName) {
            TableDefinition def = new TableDefinition(tableName, new List<TableColumn>() {
                new TableColumn("Col3", "int", allowNulls: false)
            });
            def.CreateTable();
            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values(10)");
            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values(100)");
            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values(1000)");
            return def;
        }

        internal TableDefinition CreateTableForDestination(string tableName) {
            TableDefinition def = new TableDefinition(tableName, new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: true),
                new TableColumn("Col2", "int", allowNulls: true)                
            });
            def.CreateTable();           
            return def;
        }
    }

}
