using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace ALE.ETLBoxTest {
    [TestClass]
    public class TestIssue3_DataFlowExample {
        public TestContext TestContext { get; set; }
        public string ConnectionStringParameter => TestContext?.Properties["connectionString"].ToString();
        public string DBNameParameter => TestContext?.Properties["dbName"].ToString();

        [ClassInitialize]
        public static void ClassInit(TestContext testContext) {
            TestHelper.RecreateDatabase(testContext);
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(testContext.Properties["connectionString"].ToString()));
        }

        [TestInitialize]
        public void TestInit() {
            CleanUpSchemaTask.CleanUp("dbo");
        }

        [TestMethod]
        public void DBSource_RowTrans_DBDest_Issue3() {
            SqlTask.ExecuteNonQuery("Create test table",
                @"CREATE TABLE dbo.test 
                (Col1 int null, Col2 int null, Col3 int null)"
            );
            DBSource<EntitiesInfo> source = new DBSource<EntitiesInfo>()
            {
                Sql = "SELECT * FROM (VALUES (1,2,3), (4,5,6), (7,8,9)) AS MyTable(Col1,Col2,Col3)"
            };

            RowTransformation<EntitiesInfo, EntitiesInfo> rowT = new RowTransformation<EntitiesInfo, EntitiesInfo>(
                input => new EntitiesInfo {
                    Col1 = input.Col2+ input.Col3,
                    Col2 = 0,
                    Col3 = input.Col1
                }
                );

            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(ConnectionStringParameter));
            DBDestination<EntitiesInfo> dest = new DBDestination<EntitiesInfo>("test");
            source.LinkTo(rowT);
            rowT.LinkTo(dest);
            source.Execute();
            dest.Wait();
        }
        public class EntitiesInfo {
            public int Col1 { get; set; }
            public int Col2 { get; set; }
            public int Col3 { get; set; }
        }

    }

}
