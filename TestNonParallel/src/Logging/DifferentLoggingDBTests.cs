using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBox.Logging;
using ETLBox.SqlServer;
using ETLBoxTests.Helper;
using System;
using Xunit;

namespace ETLBoxTests.Logging
{
    public class OtherDBFixture
    {
        public OtherDBFixture()
        {
            DatabaseHelper.RecreateSqlDatabase("NoLog");
        }
    }

    [Collection("Logging")]
    public class DifferentLoggingDBTests : IDisposable, IClassFixture<OtherDBFixture>
    {
        public SqlConnectionManager LoggingConnection => Config.SqlConnection.ConnectionManager("Logging");
        public SqlConnectionManager NoLogConnection => Config.SqlConnection.ConnectionManager("NoLog");
        public DifferentLoggingDBTests(LoggingDatabaseFixture dbFixture, OtherDBFixture odbFixture)
        {
            CreateLogTableTask.Create(LoggingConnection);
            ControlFlow.AddLoggingDatabaseToConfig(LoggingConnection);
        }

        public void Dispose()
        {
            DropTableTask.Drop(LoggingConnection, ControlFlow.LogTable);
            ControlFlow.ClearSettings();
            DataFlow.ClearSettings();
        }

        [Fact]
        public void ControlFlowLoggingInDifferentDB()
        {
            //Arrange

            //Act
            SqlTask.ExecuteNonQuery(NoLogConnection, "Create source table", @"CREATE TABLE CFLogSource
                            (Col1 INT NOT NULL, Col2 NVARCHAR(50) NULL)");

            ControlFlow.DefaultDbConnection = NoLogConnection;

            SqlTask.ExecuteNonQuery("Insert demo data", "INSERT INTO CFLogSource VALUES(1,'Test1')");

            //Assert
            Assert.Equal(4, new RowCountTask("etlbox_log", "task_type = 'SqlTask' ")
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

            DbSource source = new DbSource(NoLogConnection, "DFLogSource");
            DbDestination dest = new DbDestination(LoggingConnection, "DFLogDestination");

            //Act
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(4, new RowCountTask("etlbox_log", "task_type = 'DbSource'")
            {
                DisableLogging = true,
                ConnectionManager = LoggingConnection
            }.Count().Rows);
            Assert.Equal(4, new RowCountTask("etlbox_log", "task_type = 'DbDestination'")
            {
                DisableLogging = true,
                ConnectionManager = LoggingConnection
            }.Count().Rows);
        }


    }
}
