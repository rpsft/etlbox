using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;

namespace ALE.ETLBoxTest
{
    [TestClass]
    public class TestDataFlowLogging
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
            DropTableTask.Drop("Source");
            DropTableTask.Drop("Destination");
            DropTableTask.Drop("etl.Log");
            DropTableTask.Drop("etl.LoadProcess");
        }

        [TestMethod]
        public void LoggingInSourceAndDestination()
        {
            CreateLogTablesTask.CreateLog();
            StartLoadProcessTask.Start("Test");
            DataFlow.LoggingThresholdRows = 3;

            DBSource source;
            DBDestination dest;
            CreateSourceAndDestination(out source, out dest);

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            EndLoadProcessTask.End();
            Assert.AreEqual(4, RowCountTask.Count("etl.Log", "TaskType = 'DF_DBSOURCE' AND TaskAction = 'LOG' AND LoadProcessKey IS NOT NULL"));
            Assert.AreEqual(4, RowCountTask.Count("etl.Log", "TaskType = 'DF_DBDEST' AND TaskAction = 'LOG' AND LoadProcessKey IS NOT NULL"));
            Assert.AreEqual(1, RowCountTask.Count("etl.LoadProcess"));
        }

        [TestMethod]
        public void LoggingDisabled()
        {
            CreateLogTablesTask.CreateLog();
            DataFlow.LoggingThresholdRows = 0;

            DBSource source;
            DBDestination dest;
            CreateSourceAndDestination(out source, out dest);

            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            Assert.AreEqual(0, RowCountTask.Count("etl.Log", "TaskType = 'DF_DBSOURCE' AND TaskAction = 'LOG' AND LoadProcessKey IS NOT NULL"));
            Assert.AreEqual(0, RowCountTask.Count("etl.Log", "TaskType = 'DF_DBDEST' AND TaskAction = 'LOG' AND LoadProcessKey IS NOT NULL"));
        }

        private void CreateSourceAndDestination(out DBSource source, out DBDestination dest)
        {
            SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE Source
                            (Col1 nvarchar(100) null, Col2 int null)");
            for (int i = 1; i <= 10; i++)
                SqlTask.ExecuteNonQuery("Insert demo data", $"insert into Source values('Test{i}',{i})");


            SqlTask.ExecuteNonQuery("Create destination table", @"CREATE TABLE Destination
                (ID int not null identity (1,1) primary key,
                 Col1 nvarchar(30) null, Col2 nvarchar(30) null)");

            source = new DBSource("Source");
            dest = new DBDestination("Destination", 3);
        }

        [TestMethod]
        public void LoggingInRowTransformation()
        {
            CreateLogTablesTask.CreateLog();
            DataFlow.LoggingThresholdRows = 3;

            DBSource source;
            DBDestination dest;
            CreateSourceAndDestination(out source, out dest);

            RowTransformation rowTrans = new RowTransformation(row => row);
            source.LinkTo(rowTrans);
            rowTrans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            Assert.AreEqual(3, RowCountTask.Count("etl.Log", "TaskType = 'DF_ROWTRANSFORMATION' AND TaskAction = 'LOG'"));
        }
    }

}
