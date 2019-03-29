using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestDataFlowLookup {
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

        public class MyLookupRow {
            public int Key { get; set; }
            public string LookupValue { get; set; }
        }

        public class MyInputDataRow {
            public string Col1 { get; set; }
            public int Col2 { get; set; }
        }

        public class MyOutputDataRow {
            public string Col1 { get; set; }
            public string Col2 { get; set; }
        }

        /*
         * DBSource (out: MyInputDataRow)
         *      -> Lookup (in: MyInputDataRow, out: MyOutputDataRow, lookup: DBSource(out: MyLooupRow) )
         *      -> DBDestination (in: MyOutputDataRow)
         */
        [TestMethod]
        public void DB_Lookup_DB() {
            TableDefinition sourceTableDefinition = CreateDBSourceTableForInputRow();
            TableDefinition destinationTableDefinition = CreateDBDestinationTableForOutputRow();
            TableDefinition lookupTableDefinition = CreateDBLookupTable();

            TransformationTestClass testClass = new TransformationTestClass();
            DBSource<MyInputDataRow> source = new DBSource<MyInputDataRow>() { SourceTableDefinition = sourceTableDefinition };
            DBSource<MyLookupRow> lookupSource = new DBSource<MyLookupRow>() { SourceTableDefinition = lookupTableDefinition };
            Lookup<MyInputDataRow, MyOutputDataRow, MyLookupRow> lookup = new Lookup<MyInputDataRow, MyOutputDataRow,MyLookupRow>(
                testClass.TestTransformationFunc, lookupSource, testClass.LookupData
            );
            DBDestination<MyOutputDataRow> dest = new DBDestination<MyOutputDataRow>() { DestinationTableDefinition = destinationTableDefinition };
            source.LinkTo(lookup);
            lookup.LinkTo(dest);
            source.Execute();
            dest.Wait();
            Assert.AreEqual(1, RowCountTask.Count("test.Destination","Col1 = 'Test1' and Col2 = 'Lookup for 1'"));
            Assert.AreEqual(1, RowCountTask.Count("test.Destination","Col1 = 'Test2' and Col2 = 'Lookup for 2'"));
            Assert.AreEqual(1, RowCountTask.Count("test.Destination","Col1 = 'Test3' and Col2 = 'Lookup for 3'"));
        }

        internal TableDefinition CreateDBSourceTableForInputRow() {
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

        internal static TableDefinition CreateDBLookupTable() {
            TableDefinition sourceTableDefinition = new TableDefinition("test.Lookup", new List<TableColumn>() {
                new TableColumn("Key", "int", allowNulls: false),
                new TableColumn("LookupValue", "nvarchar(100)", allowNulls: true)
            });
            sourceTableDefinition.CreateTable();
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Lookup values(1, 'Lookup for 1')");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Lookup values(2, 'Lookup for 2')");
            SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Lookup values(3, 'Lookup for 3')");
            return sourceTableDefinition;
        }

        internal TableDefinition CreateDBDestinationTableForOutputRow() {
            TableDefinition destinationTableDefinition = new TableDefinition("test.Destination", new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: true),
                new TableColumn("Col2", "nvarchar(100)", allowNulls: true)
            });
            destinationTableDefinition.CreateTable();
            return destinationTableDefinition;
        }

        public class TransformationTestClass {
            public int AddValue { get; set; } = 0;

            public List<MyLookupRow> LookupData { get; set; } = new List<MyLookupRow>();

            public MyOutputDataRow TestTransformationFunc(MyInputDataRow myRow) {
                MyOutputDataRow output = new MyOutputDataRow() {
                    Col1 = myRow.Col1,
                    Col2 = LookupData.Where(ld => ld.Key == myRow.Col2).Select(ld=>ld.LookupValue).FirstOrDefault()
                };
                return output;
            }
        }
    }

}
