using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestCreateTableTask {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateSchemaTask.Create("test");
        }

        [TestMethod]
        public void TestCreateTable() {
            List<TableColumn> columns = new List<TableColumn>() { new TableColumn("value", "int") };
            CreateTableTask.Create("test.Table1", columns);
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if table exists", $"select count(*) from sys.objects where type = 'U' and object_id = object_id('test.Table1')"));

        }

        [TestMethod]
        public void TestReCreateTable() {
            List<TableColumn> columns = new List<TableColumn>() { new TableColumn("value", "int") };
            CreateTableTask.Create("test.Table1", columns);
            CreateTableTask.Create("test.Table1", columns);
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if table exists", $"select count(*) from sys.objects where type = 'U' and object_id = object_id('test.Table1')"));

        }

        [TestMethod]
        public void TestCreateTableWithNullable() {
            List<TableColumn> columns = new List<TableColumn>() { new TableColumn("value", "int"), new TableColumn("value2", "datetime", true) };
            CreateTableTask.Create("test.Table2", columns);
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if table exists", $"select count(*) from sys.objects where type = 'U' and object_id = object_id('test.Table2')"));

        }

        [TestMethod]
        public void TestCreateTableWithPrimaryKey() {
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("Key", "int",allowNulls:false,isPrimaryKey:true),
                new TableColumn("value2", "datetime", allowNulls:true)
            };
            CreateTableTask.Create("test.Table3", columns);
            Assert.AreEqual(2,SqlTask.ExecuteScalar<int>("Check if column exists", $"select count(*) from sys.columns where object_id = object_id('test.Table3')"));
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if primary key exists", $"select count(*) from sys.key_constraints where parent_object_id = object_id('test.Table3')"));
            Assert.AreEqual("pk_Table3_Key", SqlTask.ExecuteScalar("Check if primary key has correct naming", "select name from sys.key_constraints where parent_object_id = object_id('test.Table3')"));
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if column is nullable", $"select case when is_nullable = 1 then 1 else 0 end from sys.columns where object_id = object_id('test.Table3') and name='value2'"));

        }

        [TestMethod]
        public void TestCreateTableOnlyNVarChars() {
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("value1", "int",allowNulls:false),
                new TableColumn("value2", "datetime", allowNulls:true)
            };
            new CreateTableTask("test.Table4", columns.Cast<ITableColumn>().ToList()) { OnlyNVarCharColumns = true }.Execute();
            Assert.AreEqual(2, SqlTask.ExecuteScalar<int>("Check if column exists", $"select count(*) from sys.columns where object_id = object_id('test.Table4')"));
            Assert.AreEqual(2,SqlTask.ExecuteScalar<int>("Check if columns are nvarchar", $@"select count(*) from sys.columns cols inner join sys.types t on t.system_type_id = cols.system_type_id where object_id = object_id('test.Table4') and t.name = 'nvarchar'"));


        }

        [TestMethod]
        public void TestCreateTableWithIdentity() {
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("value1", "int",allowNulls:false) { IsIdentity =true, IdentityIncrement = 1000, IdentitySeed = 50 }
            };
            CreateTableTask.Create("test.Table5", columns);
            Assert.AreEqual(1, SqlTask.ExecuteScalar<int>("Check if column exists", $"select count(*) from sys.columns where object_id = object_id('test.Table5')"));
            Assert.IsTrue(SqlTask.ExecuteScalarAsBool("Check if column has identity"
                , $@"select case when is_identity = 1 then 1 else 0 end from sys.columns cols inner join sys.types t on t.system_type_id = cols.system_type_id
                     where object_id = object_id('test.Table5') and cols.name = 'value1'"));


        }

        [TestMethod]
        public void TestCreateTableWithDefault() {
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("value1", "int",allowNulls:false) { DefaultValue = "0" },
                new TableColumn("value2", "nvarchar(10)",allowNulls:false) { DefaultValue = "Test" },
                new TableColumn("value3", "decimal",allowNulls:false) { DefaultConstraintName="TestConstraint", DefaultValue = "3.12" }
            };
            CreateTableTask.Create("test.Table6", columns);
            Assert.AreEqual(3, SqlTask.ExecuteScalar<int>("Check if column exists", $"select count(*) from sys.columns where object_id = object_id('test.Table6')"));
        }


        [TestMethod]
        public void TestCreateTableWithComputedColumn() {
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("value1", "int",allowNulls:false) ,
                new TableColumn("value2", "int",allowNulls:false) ,
                new TableColumn("compValue", "bigint",allowNulls:true) { ComputedColumn = "value1 * value2" }
            };
            CreateTableTask.Create("test.Table7", columns);
            Assert.AreEqual(3, SqlTask.ExecuteScalar<int>("Check if column exists", $"select count(*) from sys.columns where object_id = object_id('test.Table7')"));
        }

        [TestMethod]
        public void TestLogging() {
            CreateLogTablesTask.CreateLog();
            CreateTableTask.Create("test.Table8", new List<TableColumn>() { new TableColumn("value", "int") });
            Assert.AreEqual(2, new SqlTask("Find log entry", "select count(*) from etl.Log where TaskType='CREATETABLE' group by TaskHash") { DisableLogging = true }.ExecuteScalar<int>());
        }

    }
}
