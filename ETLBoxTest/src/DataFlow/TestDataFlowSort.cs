using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestDataFlowBlockSort {
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

        public class MySimpleRow  {
            public string Col1 { get; set; }
            public int Col2 { get; set; }
        }


        /*
        * DSBSource (out: object) -> BlockTransformation (in/out: object) -> DBDestination (in: object)
        */
        [TestMethod]
        public void DB_BlockTrans_DB() {
            TableDefinition sourceTableDefinition = CreateSourceTable("test.Source");
            TableDefinition destinationTableDefinition = CreateDestinationTable("test.Destination");

            DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(sourceTableDefinition);
            DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(destinationTableDefinition);
            Comparison<MySimpleRow> comp = new Comparison<MySimpleRow>(
                   (x, y) => y.Col2 - x.Col2
                );
            Sort<MySimpleRow> block = new Sort<MySimpleRow>(comp);
            source.LinkTo(block);
            block.LinkTo(dest);
            source.Execute();
            dest.Wait();
            Assert.AreEqual(1, RowCountTask.Count("test.Destination", "[Key] = 1 and Col2 = 3"));
            Assert.AreEqual(1, RowCountTask.Count("test.Destination", "[Key] = 2 and Col2 = 2"));
            Assert.AreEqual(1, RowCountTask.Count("test.Destination", "[Key] = 3 and Col2 = 1"));
        }

        private static TableDefinition CreateSourceTable(string tableName) {
            TableDefinition sourceTableDefinition = new TableDefinition(tableName, new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: true)
            });
            sourceTableDefinition.CreateTable();
            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test1',1)");
            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test2',2)");
            SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test3',3)");
            return sourceTableDefinition;
        }

        private static TableDefinition CreateDestinationTable(string tableName) {
            TableDefinition destinationTableDefinition = new TableDefinition(tableName, new List<TableColumn>() {
                new TableColumn("Key", "int", allowNulls: false, isIdentity:true, isPrimaryKey:true),
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: true)
            });
            destinationTableDefinition.CreateTable();
            return destinationTableDefinition;
        }



    }

}
