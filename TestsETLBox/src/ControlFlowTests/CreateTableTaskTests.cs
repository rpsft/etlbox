using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests
{
    [Collection("ControlFlow")]
    public class CreateTableTaskTests
    {
        public SqlConnectionManager SqlConnection => Config.SqlConnectionManager("ControlFlow");
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("ControlFlow");

        public CreateTableTaskTests(ControlFlowDatabaseFixture dbFixture)
        { }

        [Theory, MemberData(nameof(Connections))]
        public void CreateTable(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() { new TableColumn("value", "INT") };
            //Act
            CreateTableTask.Create(connection, "CreateTable1", columns);
            //Assert
            Assert.True(IfExistsTask.IsExisting(connection, "CreateTable1"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void ReCreateTable(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() { new TableColumn("value", "INT") };
            CreateTableTask.Create(connection, "CreateTable2", columns);
            //Act
            CreateTableTask.Create(connection, "CreateTable2", columns);
            //Assert
            Assert.True(IfExistsTask.IsExisting(connection, "CreateTable2"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateTableWithNullable(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("value", "INT"),
                new TableColumn("value2", "DATETIME", true)
            };
            //Act
            CreateTableTask.Create(connection, "CreateTable3", columns);
            //Assert
            Assert.True(IfExistsTask.IsExisting(connection, "CreateTable3"));
            var td = TableDefinition.GetDefinitionFromTableName("CreateTable3", connection);
            Assert.Contains(td.Columns, col => col.AllowNulls);
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateTableWithPrimaryKey(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("Key", "int",allowNulls:false,isPrimaryKey:true),
                new TableColumn("value2", "datetime", allowNulls:true)
            };

            //Act
            CreateTableTask.Create(connection, "CreateTable4", columns);

            //Assert
            Assert.True(IfExistsTask.IsExisting(connection, "CreateTable4"));
            var td = TableDefinition.GetDefinitionFromTableName("CreateTable4", connection);
            Assert.Contains(td.Columns, col => col.IsPrimaryKey);

        }

        [Theory, MemberData(nameof(Connections))]
        public void ThrowingException(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("value1", "INT",allowNulls:false),
                new TableColumn("value2", "DATETIME", allowNulls:true)
            };
            CreateTableTask.Create(connection, "CreateTable5", columns);
            //Act

            //Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                new CreateTableTask("CreateTable5", columns.Cast<ITableColumn>().ToList())
                {
                    ConnectionManager = connection,
                    ThrowErrorIfTableExists = true
                }
                .Execute();
            });
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateTableWithIdentity(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("value1", "int",allowNulls:false, isPrimaryKey:false, isIdentity:true)
            };

            //Act
            CreateTableTask.Create(connection, "CreateTable6", columns);

            //Assert
            Assert.True(IfExistsTask.IsExisting(connection, "CreateTable6"));
            var td = TableDefinition.GetDefinitionFromTableName("CreateTable6", connection);
            Assert.Contains(td.Columns, col => col.IsIdentity);
        }

        [Fact]
        public void CreateTableWithIdentityIncrement()
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("value1", "int",allowNulls:false)
                {
                    IsIdentity =true,
                    IdentityIncrement = 1000,
                    IdentitySeed = 50 }
            };

            //Act
            CreateTableTask.Create(SqlConnection, "CreateTable7", columns);

            //Assert
            Assert.True(IfExistsTask.IsExisting(SqlConnection, "CreateTable7"));
            if (SqlConnection.GetType() == typeof(SqlConnectionManager))
                Assert.True(SqlTask.ExecuteScalarAsBool(SqlConnection, "Check if column has identity"
                    , $@"SELECT CASE WHEN is_identity = 1 THEN 1 ELSE 0 END FROM sys.columns cols INNER JOIN sys.types t ON t.system_type_id = cols.system_type_id
                     WHERE object_id = object_id('dbo.CreateTable7') AND cols.name = 'value1'"));
        }

        [Fact]
        public void CreateTableWithDefault()
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("value1", "int",allowNulls:false) { DefaultValue = "0" },
                new TableColumn("value2", "nvarchar(10)",allowNulls:false) { DefaultValue = "Test" },
                new TableColumn("value3", "decimal",allowNulls:false) { DefaultConstraintName="TestConstraint", DefaultValue = "3.12" }
            };
            //Act
            CreateTableTask.Create(SqlConnection, "dbo.CreateTable7", columns);
            //Assert
            Assert.Equal(3, RowCountTask.Count(SqlConnection, " sys.columns",
"object_id = object_id('dbo.CreateTable7')"));
        }


        [Fact]
        public void CreateTableWithComputedColumn()
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("value1", "int",allowNulls:false) ,
                new TableColumn("value2", "int",allowNulls:false) ,
                new TableColumn("compValue", "bigint",allowNulls:true) { ComputedColumn = "value1 * value2" }
            };
            //Act
            CreateTableTask.Create(SqlConnection, "dbo.CreateTable8", columns);
            //Assert
            Assert.Equal(3, RowCountTask.Count(SqlConnection, " sys.columns",
"object_id = object_id('dbo.CreateTable8')"));
        }
    }
}
