using ETLBox.Connection;
using ETLBox.Exceptions;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace ETLBox.ControlFlow.Tasks
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
    public class CreateTableTask : ControlFlowTask
    {
        /// <inheritdoc />
        public override string TaskName => $"Create table {TableName}";

        /// <summary>
        /// Executes the table creation.
        /// </summary>
        public void Execute()
        {
            CheckTableDefinition();
            bool tableExists = new IfTableOrViewExistsTask(TableName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (tableExists && ThrowErrorIfTableExists) throw new ETLBoxException($"Table {TableName} already exists!");
            if (!tableExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /// <summary>
        /// Executes the table creation
        /// </summary>
        public void Create() => Execute();

        /// <summary>
        /// The table definition for the table that should be created. Either use the TableDefinition or a combination of
        /// <see cref="TableName"/> and <see cref="TableColumn"/>.
        /// </summary>
        public TableDefinition TableDefinition { get; set; } = new TableDefinition();

        /// <summary>
        /// The formatted table name
        /// </summary>
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);

        /// <summary>
        /// The list of columns to create. Either use the <see cref="TableDefinition"/> or a combination of
        /// <see cref="TableName"/> and <see cref="TableColumn"/>.
        /// </summary>
        public List<TableColumn> Columns
        {
            get => TableDefinition.Columns;
            set => TableDefinition.Columns = value;
        }

        /// <summary>
        /// The name of the table to create.
        /// </summary>
        public string TableName
        {
            get => TableDefinition.Name;
            set => TableDefinition.Name = value;
        }

        /// <summary>
        /// A data type converter that is used to remap the current data type names in the TableDefintion
        /// to other, database specific type names. E.g. you can remap that the type VARCHAR(8000) is created as TEXT.
        /// </summary>
        public IDataTypeConverter DataTypeConverter { get; set; } = new DataTypeConverter();

        /// <summary>
        /// Set to true if you want an exception to be thrown when the table doesn't exists.
        /// By default the table won't be changed if it already exists.
        /// </summary>
        public bool ThrowErrorIfTableExists { get; set; }

        /// <summary>
        /// When creating the CREATE TABLE sql, ignore the Collation definition that a <see cref="TableColumn"/> potentially has.
        /// </summary>
        public bool IgnoreCollation { get; set; }

        /// <summary>
        /// The sql code that is used to create the table.
        /// </summary>
        public string Sql
        {
            get
            {
                return
$@"CREATE TABLE {TN.QuotatedFullName} (
{ColumnsDefinitionSql}
{PrimaryKeySql}
)
";
            }
        }

        public CreateTableTask()
        {

        }
        public CreateTableTask(string tableName, List<TableColumn> columns) : this()
        {
            TableDefinition = new TableDefinition(tableName, columns);
        }

        public CreateTableTask(TableDefinition tableDefinition) : this()
        {
            TableDefinition = tableDefinition;
        }

        /// <summary>
        /// Creates a table.
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columns">The columsn of the table</param>
        public static void Create(string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns).Execute();

        /// <summary>
        /// Creates a table.
        /// </summary>
        /// <param name="tableDefinition">The definition of the table containing table name and columns.</param>
        public static void Create(TableDefinition tableDefinition) => new CreateTableTask(tableDefinition).Execute();

        /// <summary>
        /// Creates a table.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableName">The name of the table</param>
        /// <param name="columns">The columsn of the table</param>
        public static void Create(IConnectionManager connectionManager, string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns) { ConnectionManager = connectionManager }.Execute();

        /// <summary>
        /// Creates a table.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableDefinition">The definition of the table containing table name and columns.</param>
        public static void Create(IConnectionManager connectionManager, TableDefinition tableDefinition) => new CreateTableTask(tableDefinition) { ConnectionManager = connectionManager }.Execute();

        string ColumnsDefinitionSql
            => String.Join("  , " + Environment.NewLine, Columns?.Select(col => CreateTableDefinition(col)));

        private void CheckTableDefinition()
        {
            if (string.IsNullOrEmpty(TableName))
                throw new ETLBoxException("No table name was provided - can not create or alter the table.");
            if (Columns == null || Columns.Count == 0)
                throw new ETLBoxException("You did not provide any columns for the table - please define at least one table column.");
            if (Columns.Any(col => string.IsNullOrEmpty(col.Name)))
                throw new ETLBoxException("One of the provided columns is either null or empty - can't create table.");
            if (Columns.Any(col => string.IsNullOrEmpty(col.DataType)))
                throw new ETLBoxException("One of the provided columns has a datatype that is either null or empty - can't create table.");
        }

        string PrimaryKeySql => CreatePrimaryKeyConstraint();
        string CreateTableDefinition(TableColumn col)
        {
            string dataType = string.Empty;
            dataType = CreateDataTypeSql(col);
            string identitySql = CreateIdentitySql(col);
            string collationSql = CreateCollationSql(col);
            string nullSql = CreateNotNullSql(col);
            string defaultSql = CreateDefaultSql(col);
            string computedColumnSql = CreateComputedColumnSql(col);
            string comment = CreateCommentSql(col);
            return $@"{QB}{col.Name}{QE} {dataType} {collationSql} {defaultSql} {identitySql} {nullSql} {computedColumnSql} {comment}";
        }

        private string CreateDataTypeSql(TableColumn col)
        {
            if (ConnectionType == ConnectionManagerType.SqlServer && col.HasComputedColumn)
                return string.Empty;
            else if (ConnectionType == ConnectionManagerType.Postgres && col.IsIdentity)
                return string.Empty;
            else
                return DataTypeConverter.TryConvertDbDataType(col.DataType, this.ConnectionType);
        }

        private string CreateIdentitySql(TableColumn col)
        {
            if (ConnectionType == ConnectionManagerType.SQLite) return string.Empty;
            else
            {
                if (col.IsIdentity)
                {
                    if (ConnectionType == ConnectionManagerType.MySql)
                        return "AUTO_INCREMENT";
                    else if (ConnectionType == ConnectionManagerType.Postgres)
                        return "SERIAL";
                    else if (ConnectionType == ConnectionManagerType.Oracle)
                        return $"GENERATED ALWAYS AS IDENTITY START WITH {col.IdentitySeed ?? 1} INCREMENT BY {col.IdentityIncrement ?? 1}";
                    return $"IDENTITY({col.IdentitySeed ?? 1},{col.IdentityIncrement ?? 1})";
                }
                else
                {
                    return string.Empty;
                }
            }
        }

        private string CreateNotNullSql(TableColumn col)
        {
            string nullSql = string.Empty;
            if (ConnectionType == ConnectionManagerType.Postgres && col.IsIdentity) return string.Empty;
            if (ConnectionType == ConnectionManagerType.Access) return string.Empty;
            if (String.IsNullOrWhiteSpace(col.ComputedColumn))
                nullSql = col.AllowNulls
                            ? "NULL"
                            : "NOT NULL";
            return nullSql;
        }

        private string CreateCollationSql(TableColumn col)
        {
            if (IgnoreCollation)
                return string.Empty;
            if (!String.IsNullOrWhiteSpace(col.Collation))
            {
                return $"COLLATE {col.Collation}";
            }
            return string.Empty;
        }

        private string CreatePrimaryKeyConstraint()
        {
            string result = string.Empty;
            if (Columns?.Any(col => col.IsPrimaryKey) ?? false)
            {
                var pkCols = Columns.Where(col => col.IsPrimaryKey);
                string pkConstName = TableDefinition.PrimaryKeyConstraintName ??
                        $"pk_{TN.UnquotatedFullName}_{string.Join("_", pkCols.Select(col => col.Name))}";
                string constraint = $"CONSTRAINT {QB}{pkConstName}{QE}";
                if (ConnectionType == ConnectionManagerType.SQLite) constraint = "";
                string pkConst = $", {constraint} PRIMARY KEY ({string.Join(",", pkCols.Select(col => $"{QB}{col.Name}{QE}"))})";
                return pkConst;
            }
            return result;
        }

        private string CreateDefaultSql(TableColumn col)
        {
            string defaultSql = string.Empty;
            if (!col.IsPrimaryKey)
                defaultSql = col.DefaultValue != null ? $" DEFAULT {SetQuotesIfString(col.DefaultValue)}" : string.Empty;
            return defaultSql;
        }

        private string CreateComputedColumnSql(TableColumn col)
        {
            if (col.HasComputedColumn && !DbConnectionManager.SupportComputedColumns)
                throw new ETLBoxNotSupportedException("Computed columns are not supported.");

            if (col.HasComputedColumn)
                return $"AS {col.ComputedColumn}";
            else
                return string.Empty;
        }

        private string CreateCommentSql(TableColumn col)
        {
            if (ConnectionType == ConnectionManagerType.MySql && !string.IsNullOrWhiteSpace(col.Comment))
                return $"COMMENT '{col.Comment}'";
            else
                return string.Empty;
        }

        string SetQuotesIfString(string value)
        {
            if (!Regex.IsMatch(value, @"^\d+(\.\d+|)$"))//@" ^ (\d|\.)+$"))
                return $"'{value}'";
            else
                return value;

        }
    }
}
