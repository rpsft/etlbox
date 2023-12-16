using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Database;
using ALE.ETLBox.src.Definitions.Exceptions;
using ALE.ETLBox.src.Toolbox.ConnectionManager.Native;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using EtlBox.Database.Tests.Infrastructure;
using Xunit.Abstractions;

namespace EtlBox.Database.Tests.ControlFlow
{
    [Collection(nameof(DatabaseCollection))]
    public abstract class CreateTableTaskTests : ControlFlowTestBase
    {
        protected CreateTableTaskTests(
            DatabaseFixture fixture,
            ConnectionManagerType connectionType,
            ITestOutputHelper logger) : base(fixture, connectionType, logger)
        {
        }

        [Fact]
        public void CreateTable()
        {
            //Arrange
            var columns = new List<TableColumn> { new("Col1", "INT") };

            //Act
            CreateTableTask.Create(ConnectionManager, "CreateTable1", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(ConnectionManager, "CreateTable1"));
        }

        [Fact]
        public void ReCreateTable()
        {
            //Arrange
            var columns = new List<TableColumn> { new("value", "INT") };
            CreateTableTask.Create(ConnectionManager, "CreateTable2", columns);
            //Act
            CreateTableTask.Create(ConnectionManager, "CreateTable2", columns);
            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(ConnectionManager, "CreateTable2"));
        }

        [Fact]
        public void CreateTableWithNullable()
        {
            //Arrange
            var columns = new List<TableColumn>
            {
                new("value", "INT"),
                new("value2", "DATE", true)
            };
            //Act
            CreateTableTask.Create(ConnectionManager, "CreateTable3", columns);
            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(ConnectionManager, "CreateTable3"));
            var td = TableDefinition.GetDefinitionFromTableName(ConnectionManager, "CreateTable3");
            Assert.Contains(td.Columns, col => col.AllowNulls);
        }

        [Fact]
        public void CreateTableWithPrimaryKey()
        {
            //Arrange
            var columns = new List<TableColumn>
            {
                new("Id", "INT", allowNulls: false, isPrimaryKey: true),
                new("value2", "DATE", allowNulls: true)
            };

            //Act
            CreateTableTask.Create(ConnectionManager, "CreateTable4", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(ConnectionManager, "CreateTable4"));
            var td = TableDefinition.GetDefinitionFromTableName(ConnectionManager, "CreateTable4");
            Assert.True(td.Columns.Count(col => col.IsPrimaryKey) == 1);
        }

        [Fact]
        public void CreateTableWithPrimaryKeyAndIndex()
        {
            //Arrange
            var columns = new List<TableColumn>
            {
                new("Id", "INT", allowNulls: false, isPrimaryKey: true),
                new("value2", "DATE", allowNulls: true)
            };
            //Act
            CreateTableTask.Create(ConnectionManager, "CreateTablePKWithIDX", columns);
            CreateIndexTask.CreateOrRecreate(
                ConnectionManager,
                "testidx",
                "CreateTablePKWithIDX",
                new List<string> { "value2" }
            );

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(ConnectionManager, "CreateTablePKWithIDX"));
            Assert.True(
                IfIndexExistsTask.IsExisting(ConnectionManager, "testidx", "CreateTablePKWithIDX")
            );
            var td = TableDefinition.GetDefinitionFromTableName(ConnectionManager, "CreateTablePKWithIDX");
            Assert.True(td.Columns.Count(col => col.IsPrimaryKey) == 1);
        }

        [Fact]
        public void CreateTableWithCompositePrimaryKey()
        {
            //Arrange
            var columns = new List<TableColumn>
            {
                new("Id1", "INT", allowNulls: false, isPrimaryKey: true),
                new("Id2", "INT", allowNulls: false, isPrimaryKey: true),
                new("value", "DATE", allowNulls: true)
            };

            //Act
            CreateTableTask.Create(ConnectionManager, "CreateTable41", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(ConnectionManager, "CreateTable41"));
            var td = TableDefinition.GetDefinitionFromTableName(ConnectionManager, "CreateTable41");
            Assert.True(
                td.Columns.Count(col => col.IsPrimaryKey && col.Name.StartsWith("Id")) == 2
            );
        }

        [Fact]
        public void ThrowingException()
        {
            //Arrange
            var columns = new List<TableColumn>
            {
                new("value1", "INT", allowNulls: false),
                new("value2", "DATE", allowNulls: true)
            };
            CreateTableTask.Create(ConnectionManager, "CreateTable5", columns);
            //Act

            //Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                new CreateTableTask("CreateTable5", columns.ToList())
                {
                    ConnectionManager = ConnectionManager,
                    ThrowErrorIfTableExists = true
                }.Execute();
            });
        }

        [Fact]
        public void CreateTableWithIdentity()
        {
            //Arrange
            var columns = new List<TableColumn>
            {
                new("value1", "INT", allowNulls: false, isPrimaryKey: true, isIdentity: true)
            };

            //Act
            CreateTableTask.Create(ConnectionManager, "CreateTable6", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(ConnectionManager, "CreateTable6"));
            if (ConnectionManager.GetType() == typeof(SQLiteConnectionManager))
            {
                return;
            }

            var td = TableDefinition.GetDefinitionFromTableName(ConnectionManager, "CreateTable6");
            Assert.Contains(td.Columns, col => col.IsIdentity);
        }

        [Fact]
        public void CreateTableWithIdentityIncrement()
        {
            //Arrange
            var columns = new List<TableColumn>
            {
                new("value1", "INT", allowNulls: false)
                {
                    IsIdentity = true,
                    IdentityIncrement = 1000,
                    IdentitySeed = 50
                }
            };

            //Act
            CreateTableTask.Create(ConnectionManager, "CreateTable7", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(ConnectionManager, "CreateTable7"));
            var td = TableDefinition.GetDefinitionFromTableName(ConnectionManager, "CreateTable7");
            Assert.Contains(
                td.Columns,
                col => col.IsIdentity && col.IdentityIncrement == 1000 && col.IdentitySeed == 50
            );
        }

        [Fact]
        public void CreateTableWithDefault()
        {
            //Arrange
            var columns = new List<TableColumn>
            {
                new("value1", "INT", allowNulls: false) { DefaultValue = "0" },
                new("value2", "NVARCHAR(10)", allowNulls: false) { DefaultValue = "Test" },
                new("value3", "DECIMAL(10,2)", allowNulls: false) { DefaultValue = "3.12" }
            };
            //Act
            CreateTableTask.Create(ConnectionManager, "CreateTable8", columns);
            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(ConnectionManager, "CreateTable8"));
            var td = TableDefinition.GetDefinitionFromTableName(ConnectionManager, "CreateTable8");
            Assert.Contains(td.Columns, col => col.DefaultValue == "0");
            Assert.Contains(td.Columns, col => col.DefaultValue is "Test" or "'Test'");
            Assert.Contains(td.Columns, col => col.DefaultValue == "3.12");
        }

        [Fact]
        public void CreateTableWithComputedColumn()
        {
            if (
                ConnectionManager.GetType() == typeof(SQLiteConnectionManager)
                || ConnectionManager.GetType() == typeof(PostgresConnectionManager)
            )
            {
                return;
            }

            //Arrange
            var columns = new List<TableColumn>
            {
                new("value1", "INT", allowNulls: false),
                new("value2", "INT", allowNulls: false),
                new("compValue", "BIGINT", allowNulls: true)
                {
                    ComputedColumn = "(value1 * value2)"
                }
            };

            //Act
            CreateTableTask.Create(ConnectionManager, "CreateTable9", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(ConnectionManager, "CreateTable9"));
            var td = TableDefinition.GetDefinitionFromTableName(ConnectionManager, "CreateTable9");
            if (ConnectionManager.GetType() == typeof(SqlConnectionManager))
                Assert.Contains(td.Columns, col => col.ComputedColumn == "[value1]*[value2]");
            else if (ConnectionManager.GetType() == typeof(MySqlConnectionManager))
                Assert.Contains(td.Columns, col => col.ComputedColumn == "(`value1` * `value2`)");
        }

        [Fact]
        public void SpecialCharsInTableName()
        {
            //Arrange
            var columns = new List<TableColumn>
            {
                new("Id1", "INT", allowNulls: false, isPrimaryKey: true),
                new("Id2", "INT", allowNulls: false, isPrimaryKey: true),
                new("value", "DATE", allowNulls: true)
            };
            string tableName;
            if (ConnectionManager.GetType() == typeof(SqlConnectionManager))
                tableName = @"[dbo].[ T""D"" 1 ]";
            else if (ConnectionManager.GetType() == typeof(PostgresConnectionManager))
                tableName = @"""public""."" T [D] 1 """;
            else if (ConnectionManager.GetType() == typeof(MySqlConnectionManager))
                tableName = @"` T [D] 1`";
            else
                tableName = @""" T [D] 1 """;
            CreateTableTask.Create(ConnectionManager, tableName, columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(ConnectionManager, tableName));
            var td = TableDefinition.GetDefinitionFromTableName(ConnectionManager, tableName);
            Assert.True(
                td.Columns.Count(col => col.IsPrimaryKey && col.Name.StartsWith("Id")) == 2
            );
        }

        [Fact]
        public void CheckExceptionHandling()
        {
            //Arrange
            var td = new TableDefinition { Name = "Test" };
            Assert.Throws<ETLBoxException>(() => CreateTableTask.Create(ConnectionManager, td));

            Assert.Throws<ETLBoxException>(
                () => CreateTableTask.Create(ConnectionManager, "", new List<TableColumn>())
            );

            Assert.Throws<ETLBoxException>(
                () => CreateTableTask.Create(ConnectionManager, "test", new List<TableColumn>())
            );

            Assert.Throws<ETLBoxException>(
                () =>
                    CreateTableTask.Create(
                        ConnectionManager,
                        "test",
                        new List<TableColumn> { new() { Name = "test" } }
                    )
            );

            Assert.Throws<ETLBoxException>(
                () =>
                    CreateTableTask.Create(
                        ConnectionManager,
                        "test",
                        new List<TableColumn> { new() { DataType = "INT" } }
                    )
            );
        }

        [Fact]
        public void CopyTableUsingTableDefinition()
        {
            //Arrange
            var columns = new List<TableColumn>
            {
                new("Id", "INT", allowNulls: false, isPrimaryKey: true, isIdentity: true),
                new("value1", "NVARCHAR(10)", allowNulls: true),
                new("value2", "DECIMAL(10,2)", allowNulls: false) { DefaultValue = "3.12" }
            };
            CreateTableTask.Create(ConnectionManager, "CreateTable10", columns);

            //Act
            var definition = TableDefinition.GetDefinitionFromTableName(
                ConnectionManager,
                "CreateTable10"
            );
            definition.Name = "CreateTable10_copy";
            CreateTableTask.Create(ConnectionManager, definition);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(ConnectionManager, "CreateTable10_copy"));
        }

        [Fact]
        public void CreateTableWithPKConstraintName()
        {
            var columns = new List<TableColumn>
            {
                new()
                {
                    Name = "ThisIsAReallyLongAndPrettyColumnNameWhichICantChange",
                    DataType = "int",
                    IsPrimaryKey = true
                },
                new() { Name = "JustRandomColumn", DataType = "int" }
            };

            var tableDefinition = new TableDefinition(
                "ThisIsAReallyLongTableWhichICantChange",
                columns
            )
            {
                PrimaryKeyConstraintName = "shortname"
            };
            CreateTableTask.Create(ConnectionManager, tableDefinition);
            var td = TableDefinition.GetDefinitionFromTableName(
                ConnectionManager,
                "ThisIsAReallyLongTableWhichICantChange"
            );
            Assert.True(
                td.Columns.Count(
                    col =>
                        col.IsPrimaryKey
                        && col.Name == "ThisIsAReallyLongAndPrettyColumnNameWhichICantChange"
                ) == 1
            );
        }

        [Fact]
        public void CreateTableWithComment()
        {
            //Arrange
            var columns = new List<TableColumn> { new("col1", "INT") { Comment = "test" } };

            //Act
            CreateTableTask.Create(ConnectionManager, "TableWithComment", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(ConnectionManager, "TableWithComment"));

            var td = TableDefinition.GetDefinitionFromTableName(
                ConnectionManager,
                "TableWithComment"
            );
            var description = td.Columns.Single(x => x.Name == "col1").Comment;

            Assert.Equal("test", description);
        }

        public class SqlSever : CreateTableTaskTests
        {
            public SqlSever(DatabaseFixture fixture, ITestOutputHelper logger)
                : base(fixture, ConnectionManagerType.SqlServer, logger)
            {
            }
        }

        public class PostgreSql : CreateTableTaskTests
        {
            public PostgreSql(DatabaseFixture fixture, ITestOutputHelper logger)
                : base(fixture, ConnectionManagerType.Postgres, logger)
            {
            }
        }
    }
}
