using ALE.ETLBox;
using ALE.ETLBox.Common;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ETLBox.Primitives;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks
{
    public class CreateTableTaskTests : ControlFlowTestBase
    {
        public CreateTableTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllSqlConnections;

        public static IEnumerable<object[]> Access => AccessConnection;

        [Theory, MemberData(nameof(Connections)), MemberData(nameof(Access))]
        public void CreateTable(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn> { new("Col1", "INT") };

            //Act
            CreateTableTask.Create(connection, "CreateTable1", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "CreateTable1"));
        }

        [Theory, MemberData(nameof(Connections)), MemberData(nameof(Access))]
        public void ReCreateTable(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn> { new("value", "INT") };
            CreateTableTask.Create(connection, "CreateTable2", columns);
            //Act
            CreateTableTask.Create(connection, "CreateTable2", columns);
            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "CreateTable2"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateTableWithNullable(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>
            {
                new("value", "INT"),
                new("value2", "DATE", true)
            };
            //Act
            CreateTableTask.Create(connection, "CreateTable3", columns);
            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "CreateTable3"));
            var td = TableDefinition.GetDefinitionFromTableName(connection, "CreateTable3");
            Assert.Contains(td.Columns, col => col.AllowNulls);
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateTableWithPrimaryKey(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>
            {
                new("Id", "INT", allowNulls: false, isPrimaryKey: true),
                new("value2", "DATE", allowNulls: true)
            };

            //Act
            CreateTableTask.Create(connection, "CreateTable4", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "CreateTable4"));
            var td = TableDefinition.GetDefinitionFromTableName(connection, "CreateTable4");
            Assert.True(td.Columns.Count(col => col.IsPrimaryKey) == 1);
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateTableWithPrimaryKeyAndIndex(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>
            {
                new("Id", "INT", allowNulls: false, isPrimaryKey: true),
                new("value2", "DATE", allowNulls: true)
            };
            //Act
            CreateTableTask.Create(connection, "CreateTablePKWithIDX", columns);
            CreateIndexTask.CreateOrRecreate(
                connection,
                "testidx",
                "CreateTablePKWithIDX",
                new List<string> { "value2" }
            );

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "CreateTablePKWithIDX"));
            Assert.True(
                IfIndexExistsTask.IsExisting(connection, "testidx", "CreateTablePKWithIDX")
            );
            var td = TableDefinition.GetDefinitionFromTableName(connection, "CreateTablePKWithIDX");
            Assert.True(td.Columns.Count(col => col.IsPrimaryKey) == 1);
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateTableWithCompositePrimaryKey(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>
            {
                new("Id1", "INT", allowNulls: false, isPrimaryKey: true),
                new("Id2", "INT", allowNulls: false, isPrimaryKey: true),
                new("value", "DATE", allowNulls: true)
            };

            //Act
            CreateTableTask.Create(connection, "CreateTable41", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "CreateTable41"));
            var td = TableDefinition.GetDefinitionFromTableName(connection, "CreateTable41");
            Assert.True(
                td.Columns.Count(col => col.IsPrimaryKey && col.Name.StartsWith("Id")) == 2
            );
        }

        [Theory, MemberData(nameof(Connections))]
        public void ThrowingException(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>
            {
                new("value1", "INT", allowNulls: false),
                new("value2", "DATE", allowNulls: true)
            };
            CreateTableTask.Create(connection, "CreateTable5", columns);
            //Act

            //Assert
            Assert.Throws<ETLBoxException>(() =>
            {
                new CreateTableTask("CreateTable5", columns.ToList())
                {
                    ConnectionManager = connection,
                    ThrowErrorIfTableExists = true
                }.Execute();
            });
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateTableWithIdentity(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>
            {
                new("value1", "INT", allowNulls: false, isPrimaryKey: true, isIdentity: true)
            };

            //Act
            CreateTableTask.Create(connection, "CreateTable6", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "CreateTable6"));
            if (connection.GetType() == typeof(SQLiteConnectionManager))
            {
                return;
            }

            var td = TableDefinition.GetDefinitionFromTableName(connection, "CreateTable6");
            Assert.Contains(td.Columns, col => col.IsIdentity);
        }

        [Fact]
        public void CreateTableWithIdentityIncrement()
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>
            {
                new("value1", "INT", allowNulls: false)
                {
                    IsIdentity = true,
                    IdentityIncrement = 1000,
                    IdentitySeed = 50
                }
            };

            //Act
            CreateTableTask.Create(SqlConnection, "CreateTable7", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(SqlConnection, "CreateTable7"));
            var td = TableDefinition.GetDefinitionFromTableName(SqlConnection, "CreateTable7");
            Assert.Contains(
                td.Columns,
                col => col.IsIdentity && col.IdentityIncrement == 1000 && col.IdentitySeed == 50
            );
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateTableWithDefault(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>
            {
                new("value1", "INT", allowNulls: false) { DefaultValue = "0" },
                new("value2", "NVARCHAR(10)", allowNulls: false) { DefaultValue = "Test" },
                new("value3", "DECIMAL(10,2)", allowNulls: false) { DefaultValue = "3.12" }
            };
            //Act
            CreateTableTask.Create(connection, "CreateTable8", columns);
            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "CreateTable8"));
            var td = TableDefinition.GetDefinitionFromTableName(connection, "CreateTable8");
            Assert.Contains(td.Columns, col => col.DefaultValue == "0");
            Assert.Contains(td.Columns, col => col.DefaultValue is "Test" or "'Test'");
            Assert.Contains(td.Columns, col => col.DefaultValue == "3.12");
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateTableWithComputedColumn(IConnectionManager connection)
        {
            if (
                connection.GetType() == typeof(SQLiteConnectionManager)
                || connection.GetType() == typeof(PostgresConnectionManager)
            )
            {
                return;
            }

            //Arrange
            List<TableColumn> columns = new List<TableColumn>
            {
                new("value1", "INT", allowNulls: false),
                new("value2", "INT", allowNulls: false),
                new("compValue", "BIGINT", allowNulls: true)
                {
                    ComputedColumn = "(value1 * value2)"
                }
            };

            //Act
            CreateTableTask.Create(connection, "CreateTable9", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "CreateTable9"));
            var td = TableDefinition.GetDefinitionFromTableName(connection, "CreateTable9");
            if (connection.GetType() == typeof(SqlConnectionManager))
                Assert.Contains(td.Columns, col => col.ComputedColumn == "[value1]*[value2]");
            else if (connection.GetType() == typeof(MySqlConnectionManager))
                Assert.Contains(td.Columns, col => col.ComputedColumn == "(`value1` * `value2`)");
        }

        [Theory, MemberData(nameof(Connections))]
        public void SpecialCharsInTableName(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>
            {
                new("Id1", "INT", allowNulls: false, isPrimaryKey: true),
                new("Id2", "INT", allowNulls: false, isPrimaryKey: true),
                new("value", "DATE", allowNulls: true)
            };
            string tableName;
            if (connection.GetType() == typeof(SqlConnectionManager))
                tableName = @"[dbo].[ T""D"" 1 ]";
            else if (connection.GetType() == typeof(PostgresConnectionManager))
                tableName = @"""public""."" T [D] 1 """;
            else if (connection.GetType() == typeof(MySqlConnectionManager))
                tableName = @"` T [D] 1`";
            else
                tableName = @""" T [D] 1 """;
            CreateTableTask.Create(connection, tableName, columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, tableName));
            var td = TableDefinition.GetDefinitionFromTableName(connection, tableName);
            Assert.True(
                td.Columns.Count(col => col.IsPrimaryKey && col.Name.StartsWith("Id")) == 2
            );
        }

        [Fact]
        public void CheckExceptionHandling()
        {
            //Arrange
            TableDefinition td = new TableDefinition { Name = "Test" };
            Assert.Throws<ETLBoxException>(() => CreateTableTask.Create(SqlConnection, td));

            Assert.Throws<ETLBoxException>(
                () => CreateTableTask.Create(SqlConnection, "", new List<TableColumn>())
            );

            Assert.Throws<ETLBoxException>(
                () => CreateTableTask.Create(SqlConnection, "test", new List<TableColumn>())
            );

            Assert.Throws<ETLBoxException>(
                () =>
                    CreateTableTask.Create(
                        SqlConnection,
                        "test",
                        new List<TableColumn> { new() { Name = "test" } }
                    )
            );

            Assert.Throws<ETLBoxException>(
                () =>
                    CreateTableTask.Create(
                        SqlConnection,
                        "test",
                        new List<TableColumn> { new() { DataType = "INT" } }
                    )
            );
        }

        [Theory, MemberData(nameof(Connections))]
        public void CopyTableUsingTableDefinition(IConnectionManager connection)
        {
            //Arrange
            List<TableColumn> columns = new List<TableColumn>
            {
                new("Id", "INT", allowNulls: false, isPrimaryKey: true, isIdentity: true),
                new("value1", "NVARCHAR(10)", allowNulls: true),
                new("value2", "DECIMAL(10,2)", allowNulls: false) { DefaultValue = "3.12" }
            };
            CreateTableTask.Create(connection, "CreateTable10", columns);

            //Act
            var definition = TableDefinition.GetDefinitionFromTableName(
                connection,
                "CreateTable10"
            );
            definition.Name = "CreateTable10_copy";
            CreateTableTask.Create(connection, definition);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(connection, "CreateTable10_copy"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void CreateTableWithPKConstraintName(IConnectionManager connection)
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
            CreateTableTask.Create(connection, tableDefinition);
            var td = TableDefinition.GetDefinitionFromTableName(
                connection,
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
            CreateTableTask.Create(MySqlConnection, "TableWithComment", columns);

            //Assert
            Assert.True(IfTableOrViewExistsTask.IsExisting(MySqlConnection, "TableWithComment"));

            var td = TableDefinition.GetDefinitionFromTableName(
                MySqlConnection,
                "TableWithComment"
            );
            var description = td.Columns.Single(x => x.Name == "col1").Comment;

            Assert.Equal("test", description);
        }
    }
}
