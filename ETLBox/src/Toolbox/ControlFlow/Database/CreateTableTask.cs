using System.Linq;
using System.Text.RegularExpressions;
using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Creates a table. If the tables exists, this task won't change the table.
    /// </summary>
    /// <example>
    /// <code>
    /// CreateTableTask.Create("demo.table1", new List&lt;TableColumn&gt;() {
    /// new TableColumn(name:"key", dataType:"int", allowNulls:false, isPrimaryKey:true, isIdentity:true),
    ///     new TableColumn(name:"value", dataType:"nvarchar(100)", allowNulls:true)
    /// });
    /// </code>
    /// </example>
    [PublicAPI]
    public class CreateTableTask : GenericTask
    {
        /* ITask Interface */
        public override string TaskName => $"Create table {TableName}";

        public void Execute()
        {
            CheckTableDefinition();

            bool tableExists = new IfTableOrViewExistsTask(TableName)
            {
                ConnectionManager = ConnectionManager,
                DisableLogging = true
            }.Exists();

            switch (tableExists)
            {
                case true when ThrowErrorIfTableExists:
                    throw new ETLBoxException($"Table {TableName} already exists!");
                case false:
                    new SqlTask(this, Sql).ExecuteNonQuery();
                    break;
            }
        }

        /* Public properties */
        public void Create() => Execute();

        public TableDefinition TableDefinition { get; set; }
        public ObjectNameDescriptor TN => new(TableName, QB, QE);
        public List<TableColumn> Columns
        {
            get => TableDefinition.Columns;
            set => TableDefinition.Columns = value;
        }
        public string TableName
        {
            get => TableDefinition.Name;
            set => TableDefinition.Name = value;
        }

        public bool ThrowErrorIfTableExists { get; set; }

        public string Sql
        {
            get
            {
                return $@"CREATE TABLE {TN.QuotedFullName} (
{ColumnsDefinitionSql}
{PrimaryKeySql}
)
";
            }
        }

        public CreateTableTask() { }

        public CreateTableTask(string tableName, List<TableColumn> columns)
            : this()
        {
            TableDefinition = new TableDefinition(tableName, columns);
        }

        public CreateTableTask(TableDefinition tableDefinition)
            : this()
        {
            TableDefinition = tableDefinition;
        }

        public static void Create(string tableName, List<TableColumn> columns) =>
            new CreateTableTask(tableName, columns).Execute();

        public static void Create(TableDefinition tableDefinition) =>
            new CreateTableTask(tableDefinition).Execute();

        public static void Create(
            IConnectionManager connectionManager,
            string tableName,
            List<TableColumn> columns
        ) =>
            new CreateTableTask(tableName, columns)
            {
                ConnectionManager = connectionManager
            }.Execute();

        public static void Create(
            IConnectionManager connectionManager,
            TableDefinition tableDefinition
        ) =>
            new CreateTableTask(tableDefinition)
            {
                ConnectionManager = connectionManager
            }.Execute();

        private string ColumnsDefinitionSql =>
            Columns == null
                ? string.Empty
                : string.Join("  , " + Environment.NewLine, Columns.Select(CreateTableDefinition));

        private void CheckTableDefinition()
        {
            if (string.IsNullOrEmpty(TableName))
                throw new ETLBoxException(
                    "No table name was provided - can not create or alter the table."
                );
            if (Columns == null || Columns.Count == 0)
                throw new ETLBoxException(
                    "You did not provide any columns for the table - please define at least one table column."
                );
            if (Columns.Exists(col => string.IsNullOrEmpty(col.Name)))
                throw new ETLBoxException(
                    "One of the provided columns is either null or empty - can't create table."
                );
            if (Columns.Exists(col => string.IsNullOrEmpty(col.DataType)))
                throw new ETLBoxException(
                    "One of the provided columns has a datatype that is either null or empty - can't create table."
                );
        }

        private string PrimaryKeySql => CreatePrimaryKeyConstraint();

        private string CreateTableDefinition(ITableColumn col)
        {
            var dataType = CreateDataTypeSql(col);
            string identitySql = CreateIdentitySql(col);
            string collationSql = !string.IsNullOrWhiteSpace(col.Collation)
                ? $"COLLATE {col.Collation}"
                : string.Empty;
            string nullSql = CreateNotNullSql(col);
            string defaultSql = CreateDefaultSql(col);
            string computedColumnSql = CreateComputedColumnSql(col);
            string comment = CreateCommentSql(col);
            return $@"{QB}{col.Name}{QE} {dataType} {collationSql} {defaultSql} {identitySql} {nullSql} {computedColumnSql} {comment}";
        }

        private string CreateDataTypeSql(ITableColumn col)
        {
            return ConnectionType switch
            {
                ConnectionManagerType.SqlServer when col.HasComputedColumn => string.Empty,
                ConnectionManagerType.Postgres when col.IsIdentity => string.Empty,
                _ => DataTypeConverter.TryGetDBSpecificType(col.DataType, ConnectionType)
            };
        }

        private string CreateIdentitySql(ITableColumn col)
        {
            if (ConnectionType == ConnectionManagerType.SQLite || !col.IsIdentity)
            {
                return string.Empty;
            }

            return ConnectionType switch
            {
                ConnectionManagerType.MySql => "AUTO_INCREMENT",
                ConnectionManagerType.Postgres => "SERIAL",
                _ => $"IDENTITY({col.IdentitySeed ?? 1},{col.IdentityIncrement ?? 1})"
            };
        }

        private string CreateNotNullSql(ITableColumn col)
        {
            switch (ConnectionType)
            {
                case ConnectionManagerType.Postgres when col.IsIdentity:
                case ConnectionManagerType.Access:
                    return string.Empty;
                default:
                    if (string.IsNullOrWhiteSpace(col.ComputedColumn))
                        return col.AllowNulls ? "NULL" : "NOT NULL";
                    return string.Empty;
            }
        }

        private string CreatePrimaryKeyConstraint()
        {
            string result = string.Empty;
            if (!(Columns?.Exists(col => col.IsPrimaryKey) ?? false))
            {
                return result;
            }

            var pkCols = Columns.Where(col => col.IsPrimaryKey).ToArray();
            string pkConstName =
                TableDefinition.PrimaryKeyConstraintName
                ?? $"pk_{TN.UnquotedFullName}_{string.Join("_", pkCols.Select(col => col.Name))}";
            string constraint = $"CONSTRAINT {QB}{pkConstName}{QE}";
            if (ConnectionType == ConnectionManagerType.SQLite)
                constraint = "";
            string pkConst =
                $", {constraint} PRIMARY KEY ({string.Join(",", pkCols.Select(col => $"{QB}{col.Name}{QE}"))})";
            return pkConst;
        }

        private static string CreateDefaultSql(ITableColumn col)
        {
            string defaultSql = string.Empty;
            if (!col.IsPrimaryKey)
                defaultSql =
                    col.DefaultValue != null
                        ? $" DEFAULT {SetQuotesIfString(col.DefaultValue)}"
                        : string.Empty;
            return defaultSql;
        }

        private string CreateComputedColumnSql(ITableColumn col)
        {
            return col.HasComputedColumn switch
            {
                true when !DbConnectionManager.SupportComputedColumns
                    => throw new ETLBoxNotSupportedException("Computed columns are not supported."),
                true => $"AS {col.ComputedColumn}",
                _ => string.Empty
            };
        }

        private string CreateCommentSql(ITableColumn col)
        {
            if (
                ConnectionType == ConnectionManagerType.MySql
                && !string.IsNullOrWhiteSpace(col.Comment)
            )
                return $"COMMENT '{col.Comment}'";
            return string.Empty;
        }

        private static string SetQuotesIfString(string value)
        {
            return !Regex.IsMatch(value, @"^\d+(\.\d+|)$") //@" ^ (\d|\.)+$"))
                ? $"'{value}'"
                : value;
        }
    }
}