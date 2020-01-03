using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.ControlFlow.SqlServer;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests.SqlServer
{
    [Collection("ControlFlow")]
    public class CleanUpSchemaTaskTests
    {
        public SqlConnectionManager Connection => Config.SqlConnection.ConnectionManager("ControlFlow");
        public CleanUpSchemaTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Fact]
        public void CleanUpSchema()
        {
            //Arrange
            string schemaName = "s" + HashHelper.RandomString(9);
            SqlTask.ExecuteNonQuery(Connection, "Create schema",
                $"CREATE SCHEMA {schemaName}");
            SqlTask.ExecuteNonQuery(Connection, "Create table",
                $"CREATE TABLE {schemaName}.Table1 ( Nothing INT NULL )");
            SqlTask.ExecuteNonQuery(Connection, "Create view",
                $"CREATE VIEW {schemaName}.View1 AS SELECT * FROM {schemaName}.Table1");
            SqlTask.ExecuteNonQuery(Connection, "Create procedure",
                $"CREATE PROCEDURE {schemaName}.Proc1 AS SELECT * FROM {schemaName}.Table1");
            var objCountSql = new SqlTask("Count object",
                $@"SELECT COUNT(*) FROM sys.objects obj 
 INNER JOIN sys.schemas sch  ON sch.schema_id = obj.schema_id
WHERE sch.name = '{schemaName}'")
            { ConnectionManager = Connection };
            Assert.Equal(3, objCountSql.ExecuteScalar<int>());

            //Act
            CleanUpSchemaTask.CleanUp(Connection, schemaName);

            //Assert
            Assert.Equal(0, objCountSql.ExecuteScalar<int>());
        }

        [Fact]
        public void CleanupETLLogTablesWhileLogging()
        {
            //Arrange
            CreateLogTableTask.Create(Connection);
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.tables",
                $"type = 'U' AND name = 'Log' AND schema_id('etl') = schema_id"));
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.tables",
                $"type = 'U' AND name = 'LoadProcess' AND schema_id('etl') = schema_id"));

            //Act
            CleanUpSchemaTask.CleanUp(Connection, "etl");

            //Assert
            Assert.Equal(0, RowCountTask.Count(Connection, "sys.tables",
               $"type = 'U' AND name = 'Log' AND schema_id('etl') = schema_id"));
            Assert.Equal(0, RowCountTask.Count(Connection, "sys.tables",
                $"type = 'U' AND name = 'LoadProcess' AND schema_id('etl') = schema_id"));
        }

        [Fact]
        public void NotSupportedWithOtherDBs()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => CleanUpSchemaTask.CleanUp(Config.SQLiteConnection.ConnectionManager("ControlFlow"), "Test")
                );
        }
    }
}
