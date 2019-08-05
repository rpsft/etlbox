using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestTruncateTable {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void TestInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
            CreateSchemaTask.Create("test");
            SqlTask.ExecuteNonQuery("Create test data table",$@"
if object_id('dbo.TRC') is not null drop table dbo.TRC
create table dbo.TRC ( value int null )
insert into dbo.TRC select * from (values (1), (2), (3)) AS MyTable(v)");
        }

        [TestMethod]
        public void TestTruncate() {
            Assert.AreEqual(3, RowCountTask.Count("dbo.TRC"));
            TruncateTableTask.Truncate("dbo.TRC");
            Assert.AreEqual(0, RowCountTask.Count("dbo.TRC"));
        }

        [TestMethod]
        public void TestLogging() {
            CreateLogTablesTask.CreateLog();
            TruncateTableTask.Truncate("dbo.TRC");
            Assert.IsTrue(new SqlTask("Find log entry", "select count(*) from etl.Log where TaskAction = 'START' and TaskType='TRUNCATE'") { DisableLogging = true }.ExecuteScalarAsBool());
        }

    }
}
