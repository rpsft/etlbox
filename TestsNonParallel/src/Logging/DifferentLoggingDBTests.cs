using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ALE.ETLBoxTests.Logging
{
    public class OtherDBFixture
    {
        public OtherDBFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("NoLog");
            ControlFlow.SetLoggingDatabase(Config.SqlConnectionManager("NoLog"));
        }
    }

    [Collection("Logging")]
    public class DifferentLoggingDBTests : IDisposable, IClassFixture<OtherDBFixture>
    {
        public SqlConnectionManager LoggingConnection => Config.SqlConnectionManager("Logging");
        public SqlConnectionManager NoLogConnection => Config.SqlConnectionManager("NoLog");
        public DifferentLoggingDBTests(LoggingDatabaseFixture dbFixture, OtherDBFixture odbFixture)
        {
            ControlFlow.ClearSettings();
            ControlFlow.CurrentDbConnection = NoLogConnection;
            CreateLogTablesTask.CreateLog(LoggingConnection);
        }

        public void Dispose()
        {
            RemoveLogTablesTask.Remove(LoggingConnection);
            ControlFlow.ClearSettings();
            DataFlow.ClearSettings();
        }

        [Fact]
        public void ControlFlowLoggingInDifferentDB()
        {
            //Arrange

            //Act
            ControlFlow.CurrentDbConnection = NoLogConnection;
            SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE CFLogSource
                            (Col1 INT NOT NULL, Col2 NVARCHAR(50) NULL)");
            SqlTask.ExecuteNonQuery("Insert demo data", "INSERT INTO CFLogSource VALUES(1,'Test1')");

            ControlFlow.CurrentDbConnection = LoggingConnection;

            SqlTask.ExecuteNonQuery(NoLogConnection, "Insert demo data", "INSERT INTO CFLogSource VALUES(2,'Test2')");
            SqlTask.ExecuteNonQuery(NoLogConnection, "Insert demo data", "INSERT INTO CFLogSource VALUES(3,'Test3')");

            //Assert
            Assert.Equal(4, new RowCountTask("etl.Log", "TaskType = 'SqlTask' ")
            {
                DisableLogging = true,
                ConnectionManager = LoggingConnection
            }.Count().Rows);
        }

        [Fact]
        public void DataFlowLoggingInDifferentDB()
        {
            //Arrange
            DataFlow.LoggingThresholdRows = 3;
            SqlTask.ExecuteNonQuery(NoLogConnection, "Create source table", @"CREATE TABLE DFLogSource
                            (Col1 INT NOT NULL, Col2 NVARCHAR(50) NULL)");
            SqlTask.ExecuteNonQuery(NoLogConnection, "Insert demo data", "INSERT INTO DFLogSource VALUES(1,'Test1')");
            SqlTask.ExecuteNonQuery(NoLogConnection, "Insert demo data", "INSERT INTO DFLogSource VALUES(2,'Test2')");
            SqlTask.ExecuteNonQuery(NoLogConnection, "Insert demo data", "INSERT INTO DFLogSource VALUES(3,'Test3')");

            SqlTask.ExecuteNonQuery(LoggingConnection, "Create source table", @"CREATE TABLE DFLogDestination
                            (Col1 INT NOT NULL, Col2 NVARCHAR(50) NULL)");

            DBSource source = new DBSource(NoLogConnection, "DFLogSource");
            DBDestination dest = new DBDestination(LoggingConnection, "DFLogDestination");

            //Act
            ControlFlow.CurrentDbConnection = LoggingConnection;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(4, new RowCountTask("etl.Log", "TaskType = 'DBSource'")
            {
                DisableLogging = true,
                ConnectionManager = LoggingConnection
            }.Count().Rows);
            Assert.Equal(4, new RowCountTask("etl.Log", "TaskType = 'DBDestination'")
            {
                DisableLogging = true,
                ConnectionManager = LoggingConnection
            }.Count().Rows);
        }


    }
}
