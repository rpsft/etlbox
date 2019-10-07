using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxTests.Logging
{
    [Collection("Logging")]
    public class DataFlowLoggingTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("Logging");
        public DataFlowLoggingTests(LoggingDatabaseFixture dbFixture)
        {
            CreateLogTablesTask.CreateLog(Connection);
        }

        public void Dispose()
        {
            RemoveLogTablesTask.Remove(Connection);
            ControlFlow.ClearSettings();
            DataFlow.ClearSettings();
        }

        private void CreateTestTable(string tableName)
        {
            new DropTableTask(tableName)
            {
                ConnectionManager = Connection,
                DisableLogging = true
            }.DropIfExists();

            new CreateTableTask(new TableDefinition(tableName, new List<TableColumn>() {
                new TableColumn("Col1", "INT", allowNulls: false),
                new TableColumn("Col2", "NVARCHAR(100)", allowNulls: true)
            }))
            {
                ConnectionManager = Connection,
                DisableLogging = true
            }.Create();
        }

        private void InsertTestData(string tableName)
        {
            for (int i = 0; i < 10; i++)
                new SqlTask("Insert demo data", $"INSERT INTO {tableName} VALUES({i},'Test{i}')")
                {
                    ConnectionManager = Connection,
                    DisableLogging = true
                }.ExecuteNonQuery();
        }

        [Fact]
        public void SourceAndDestinationLogging()
        {
            //Arrange
            CreateTestTable("DBSource");
            InsertTestData("DBSource");
            CreateTestTable("DBDestination");
            DBSource source = new DBSource(Connection, "DBSource");
            DBDestination dest = new DBDestination(Connection, "DBDestination", batchSize: 3);

            //Act
            StartLoadProcessTask.Start(Connection, "Test");
            DataFlow.LoggingThresholdRows = 3;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(4, new RowCountTask("etl.Log", "TaskType = 'DBSource' AND TaskAction = 'LOG' AND LoadProcessKey IS NOT NULL")
            {
                DisableLogging = true,
                ConnectionManager = Connection
            }.Count().Rows);
            Assert.Equal(4, new RowCountTask("etl.Log", "TaskType = 'DBDestination' AND TaskAction = 'LOG' AND LoadProcessKey IS NOT NULL")
            {
                DisableLogging = true,
                ConnectionManager = Connection
            }.Count().Rows);
        }

        [Fact]
        public void LoggingReduced()
        {
            //Arrange
            CreateTestTable("DBSource");
            InsertTestData("DBSource");
            CreateTestTable("DBDestination");
            DBSource source = new DBSource(Connection, "DBSource");
            DBDestination dest = new DBDestination(Connection, "DBDestination", batchSize: 3);

            //Act
            DataFlow.LoggingThresholdRows = 0;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();
            DataFlow.LoggingThresholdRows = 0;

            //Assert

            Assert.Equal(2, new RowCountTask("etl.Log", "TaskType = 'DBSource'")
                { ConnectionManager = Connection, DisableLogging = true }.Count().Rows);
            Assert.Equal(2, new RowCountTask("etl.Log", "TaskType = 'DBDestination'")
            { ConnectionManager = Connection, DisableLogging = true }.Count().Rows);
        }

        [Fact]
        public void LoggingInRowTransformation()
        {
            //Arrange
            CreateTestTable("DBSource");
            InsertTestData("DBSource");
            CreateTestTable("DBDestination");
            DBSource source = new DBSource(Connection, "DBSource");
            DBDestination dest = new DBDestination(Connection, "DBDestination", batchSize: 3);
            RowTransformation rowTrans = new RowTransformation(row => row);

            //Act
            DataFlow.LoggingThresholdRows = 3;
            source.LinkTo(rowTrans);
            rowTrans.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, new RowCountTask("etl.Log", "TaskType = 'RowTransformation' AND TaskAction = 'LOG'")
            {
                DisableLogging = true,
                ConnectionManager = Connection
            }.Count().Rows);
         }

        [Fact]
        public void LoggingInCSVSource()
        {
            //Arrange
            CreateTestTable("DBDestination");
            CSVSource source = new CSVSource("res/DataFlowLogging/TwoColumns.csv");
            DBDestination dest = new DBDestination(Connection, "DBDestination", batchSize: 3);

            //Act
            DataFlow.LoggingThresholdRows = 2;
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(4, new RowCountTask("etl.Log", "TaskType = 'CSVSource' ")
            {
                DisableLogging = true,
                ConnectionManager = Connection
            }.Count().Rows);
        }
    }
}
