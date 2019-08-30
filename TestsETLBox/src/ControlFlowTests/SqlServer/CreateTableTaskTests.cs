using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace ALE.ETLBoxTests.ControlFlowTests.SqlServer
{
    [Collection("Sql Server ControlFlow")]
    public class CreateTableTaskTests
    {
        public SqlConnectionManager Connection => Config.SqlConnectionManager("ControlFlow");
        public CreateTableTaskTests(DatabaseFixture dbFixture)
        { }

        [Fact]
        public void CreateTable()
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() { new TableColumn("value", "int") };
            //Act
            CreateTableTask.Create(Connection, "dbo.CreateTable1", columns);
            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.objects",
                "type = 'U' AND object_id = object_id('dbo.CreateTable1')"));
        }

        [Fact]
        public void ReCreateTable()
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() { new TableColumn("value", "int") };
            CreateTableTask.Create(Connection, "dbo.CreateTable2", columns);
            //Act
            CreateTableTask.Create(Connection, "dbo.CreateTable2", columns);
            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.objects",
                "type = 'U' AND object_id = object_id('dbo.CreateTable2')"));
        }

        [Fact]
        public void CreateTableWithNullable()
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() { new TableColumn("value", "int"), new TableColumn("value2", "datetime", true) };
            //Act
            CreateTableTask.Create(Connection, "dbo.CreateTable3", columns);
            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.objects",
                "type = 'U' AND object_id = object_id('dbo.CreateTable3')"));
       }

        [Fact]
        public void CreateTableWithPrimaryKey()
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("Key", "int",allowNulls:false,isPrimaryKey:true),
                new TableColumn("value2", "datetime", allowNulls:true)
            };
            //Act
            CreateTableTask.Create(Connection, "dbo.CreateTable4", columns);
            //Assert
            Assert.Equal(2, RowCountTask.Count(Connection, " sys.columns",
                "object_id = object_id('dbo.CreateTable4')"));
            Assert.Equal(1, RowCountTask.Count(Connection, "sys.key_constraints",
                "parent_object_id = object_id('dbo.CreateTable4')"));
            Assert.Equal("pk_CreateTable4_Key",
                SqlTask.ExecuteScalar(Connection, "Check if primary key has correct naming",
                "SELECT name FROM sys.key_constraints WHERE parent_object_id = object_id('dbo.CreateTable4')"));
            Assert.True(SqlTask.ExecuteScalarAsBool(Connection, "Check if column is nullable",
                $"SELECT CASE WHEN is_nullable = 1 THEN 1 ELSE 0 END FROM sys.columns WHERE object_id = object_id('dbo.CreateTable4') AND name='value2'"));

        }

        [Fact]
        public void CreateTableOnlyNVarChars()
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("value1", "int",allowNulls:false),
                new TableColumn("value2", "datetime", allowNulls:true)
            };
            //Act
            new CreateTableTask("dbo.CreateTable5", columns.Cast<ITableColumn>().ToList())
            {
                OnlyNVarCharColumns = true,
                ConnectionManager = Connection
            }
            .Execute();
            //Assert
            Assert.Equal(2, RowCountTask.Count(Connection, " sys.columns",
     "object_id = object_id('dbo.CreateTable5')"));
            Assert.Equal(2, SqlTask.ExecuteScalar<int>(Connection, "Check if columns are nvarchar",
                $@"SELECT COUNT(*) FROM sys.columns cols INNER JOIN sys.types t ON t.system_type_id = cols.system_type_id WHERE object_id = object_id('dbo.CreateTable5') AND t.name = 'nvarchar'"));


        }

        [Fact]
        public void CreateTableWithIdentity()
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("value1", "int",allowNulls:false) { IsIdentity =true, IdentityIncrement = 1000, IdentitySeed = 50 }
            };
            //Act
            CreateTableTask.Create(Connection, "dbo.CreateTable6", columns);
            //Assert
            Assert.Equal(1, RowCountTask.Count(Connection, " sys.columns",
"object_id = object_id('dbo.CreateTable6')"));
            Assert.True(SqlTask.ExecuteScalarAsBool(Connection, "Check if column has identity"
                , $@"SELECT CASE WHEN is_identity = 1 THEN 1 ELSE 0 END FROM sys.columns cols INNER JOIN sys.types t ON t.system_type_id = cols.system_type_id
                     WHERE object_id = object_id('dbo.CreateTable6') AND cols.name = 'value1'"));


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
            CreateTableTask.Create(Connection, "dbo.CreateTable7", columns);
            //Assert
            Assert.Equal(3, RowCountTask.Count(Connection, " sys.columns",
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
            CreateTableTask.Create(Connection, "dbo.CreateTable8", columns);
            //Assert
            Assert.Equal(3, RowCountTask.Count(Connection, " sys.columns",
"object_id = object_id('dbo.CreateTable8')"));
        }
    }
}
