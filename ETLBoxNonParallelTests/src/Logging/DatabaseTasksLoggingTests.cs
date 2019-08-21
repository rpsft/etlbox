using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ALE.ETLBoxLoggingTests
{
    [Collection("Logging")]
    public class DatabaseTasksLoggingTests : IDisposable
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("Logging");
        public DatabaseTasksLoggingTests(LoggingDatabaseFixture dbFixture)
        {
            CreateLogTablesTask.CreateLog(Connection);
        }

        public void Dispose()
        {
            RemoveLogTablesTask.Remove(Connection);
        }

        private int? CountLogEntries(string taskname)
        {
            return new SqlTask("Find log entry", $@"
SELECT COUNT(*) FROM etl.Log
WHERE TaskType='{taskname}'
GROUP BY TaskHash")
            {
                DisableLogging = true,
                ConnectionManager = Connection
            }
            .ExecuteScalar<int>();
        }

        private void CreateSimpleTable(string tableName)
        {
            new SqlTask("Create test data table", $@"
CREATE TABLE {tableName}
(
    Col1 INT NULL,
    Col2 NVARCHAR(50) NULL
)
INSERT INTO {tableName}
SELECT * FROM
(VALUES (1,'Test1'), (2,'Test2'), (3,'Test3')) AS MyTable(v,w)")
            {
                ConnectionManager = Config.SqlConnectionManager("Logging"),
                DisableLogging = true
            }.ExecuteNonQuery();
        }


        [Fact]
        public void RowCountLogging()
        {
            //Arrange
            CreateSimpleTable("etl.RowCountLog");
            //Act
            RowCountTask.Count(Connection, "etl.RowCountLog");
            //Assert
            Assert.Equal(2, CountLogEntries("ROWCOUNT"));
        }
        [Fact]
        public void RowCountWithConditionLogging()
        {
            //Arrange
            CreateSimpleTable("etl.RowCountWithCondition");
            //Act
            RowCountTask.Count(Connection, "etl.RowCountWithCondition", "Col1 = 2");
            //Assert
            Assert.Equal(2, new SqlTask("Find log entry",
               @"
SELECT COUNT(*) FROM etl.Log 
WHERE TaskType='ROWCOUNT' 
AND Message LIKE '%with condition%' 
GROUP BY TaskHash")
            { DisableLogging = true, ConnectionManager = Connection }.ExecuteScalar<int>());
        }

        [Fact]
        public void SqlTaskLogging()
        {
            //Arrange
            //Act
            SqlTask.ExecuteNonQuery(Connection, "Test select", $"select 1 as test");
            //Assert
            Assert.Equal(2, CountLogEntries("SQL"));
        }

        [Fact]
        public void TruncateTableLogging()
        {
            //Arrange
            CreateSimpleTable("etl.TruncateTableLog");
            //Act
            TruncateTableTask.Truncate(Connection, "etl.TruncateTableLog");
            //Assert
            Assert.Equal(2, CountLogEntries("TRUNCATE"));
        }

        [Fact]
        public void DropTableLogging()
        {
            //Arrange
            CreateSimpleTable("etl.DropTableLog");
            //Act
            DropTableTask.Drop(Connection, "etl.DropTableLog");
            //Assert
            Assert.Equal(2, CountLogEntries("DROPTABLE"));
        }

        [Fact]
        public void CRUDViewLogging()
        {
            //Arrange
            //Act
            CRUDViewTask.CreateOrAlter(Connection, "dbo.CRUDView", "SELECT 1 AS Test");
            //Assert
            Assert.Equal(4, CountLogEntries("CRUDVIEW"));
        }

        [Fact]
        public void CRUDProcedureLogging()
        {
            //Arrange
            //Act
            CRUDProcedureTask.CreateOrAlter(Connection, "dbo.Proc1", "SELECT 1 AS Test");
            //Assert
            Assert.Equal(4, CountLogEntries("CRUDPROC"));
        }

        [Fact]
        public void CreateTableLogging()
        {
            //Arrange
            //Act
            CreateTableTask.Create(Connection, "dbo.CreateTable",
                new List<TableColumn>() { new TableColumn("value", "INT") });
            //Assert
            Assert.Equal(2, CountLogEntries("CREATETABLE"));
        }

        [Fact]
        public void CreateSchemaLogging()
        {
            //Arrange
            //Act
            CreateSchemaTask.Create(Connection, "createdSchema");
            //Assert
            Assert.Equal(2, CountLogEntries("CREATESCHEMA"));
        }

        [Fact]
        public void CreateIndexLogging()
        {
            //Arrange
            CreateSimpleTable("dbo.CreateIndex");
            //Act
            CreateIndexTask.Create(Connection, "ix_logIndexTest", "dbo.CreateIndex",
                new List<string>() { "Col1", "Col2" });
            //Assert
            Assert.Equal(2, CountLogEntries("CREATEINDEX"));
        }

        [Fact]
        public void CheckHashValuesEquality()
        {
            //Arrange
            SqlTask.ExecuteNonQuery(Connection, "Test Task - same name", "Select 1 as test");
            //Act
            SqlTask.ExecuteNonQuery(Connection, "Test Task - same name", "Select 2 as test");
            //Assert
            Assert.Equal(4, new SqlTask("Check if hash are equal",
                $@"select count(*) from etl.Log group by TaskHash")
            {
                DisableLogging = true,
                ConnectionManager = Connection
            }.ExecuteScalar<int>());
        }

        [Fact]
        public void CustomTaskLogging()
        {
            //Arrange
            //Act
            CustomTask.Execute("Test custom task 4", () => { });
            //Assert
            Assert.Equal(2, CountLogEntries("CUSTOM"));
        }

        [Fact]
        public void SequenceLogging()
        {
            //Arrange
            //Act
            Sequence.Execute("Test sequence 3", () => { });
            //Assert
            Assert.Equal(2, CountLogEntries("SEQUENCE"));
        }
    }
}
