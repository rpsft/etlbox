using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.Exceptions;
using ETLBoxTests.Fixtures;
using ETLBoxTests.Helper;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace ETLBoxTests.ControlFlowTests.Oracle
{
    [Collection("ControlFlow")]
    public class CopyTablesByDefinitionTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("ControlFlow");
        public OracleConnectionManager OracleConnection => Config.OracleConnection.ConnectionManager("ControlFlow");


        public CopyTablesByDefinitionTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Fact]
        public void CopyTableFromOracleToSqlServer()
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("Id", "INT",allowNulls:false, isPrimaryKey:true, isIdentity:true),
                new TableColumn("value1", "NVARCHAR(10)",allowNulls:true),
                new TableColumn("value2", "DECIMAL(10,2)",allowNulls:false),
                   new TableColumn("value3", "DATE",allowNulls:false)
            };
            CreateTableTask.Create(OracleConnection, "CopyTable_Oracle2Sql", columns);

            //Act
            var definition =
                TableDefinition.FromTableName(OracleConnection, "CopyTable_Oracle2Sql");
            definition.Name = "CopyTable_Oracle2Sql_copy";
            var ct = new CreateTableTask(definition)
            {
                IgnoreCollation = true,
                ConnectionManager = SqlConnection
            };
            ct.Create();

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(SqlConnection, "CopyTable_Oracle2Sql_copy"));
        }
    }
}
