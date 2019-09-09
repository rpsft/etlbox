using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

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
    public class CreateTableTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CREATETABLE";
        public override string TaskName => $"Create table {TableName}";
        public override void Execute()
        {
            bool tableExists = new IfExistsTask(TableName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (tableExists && ThrowErrorIfTableExists) throw new ETLBoxException($"Table {TableName} already exists!");
            if (!tableExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public void Create() => Execute();
        public string TableName { get; set; }
        public string TableWithoutSchema => TableName.IndexOf('.') > 0 ? TableName.Substring(TableName.LastIndexOf('.') + 1) : TableName;
        public IList<ITableColumn> Columns { get; set; }
        public bool ThrowErrorIfTableExists { get; set; }

        public string Sql
        {
            get
            {
                return
$@"CREATE TABLE {TableName} (
  {ColumnsDefinitionSql}
  )
";
            }
        }

        public CreateTableTask()
        {

        }
        public CreateTableTask(string tableName, IList<ITableColumn> columns) : this()
        {
            this.TableName = tableName;
            this.Columns = columns;
        }

        public CreateTableTask(TableDefinition tableDefinition) : this()
        {
            this.TableName = tableDefinition.Name;
            this.Columns = tableDefinition.Columns.Cast<ITableColumn>().ToList();
        }

        public static void Create(string tableName, IList<ITableColumn> columns) => new CreateTableTask(tableName, columns).Execute();
        public static void Create(string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns.Cast<ITableColumn>().ToList()).Execute();
        public static void Create(TableDefinition tableDefinition) => new CreateTableTask(tableDefinition).Execute();
        public static void Create(IConnectionManager connectionManager, string tableName, IList<ITableColumn> columns) => new CreateTableTask(tableName, columns) { ConnectionManager = connectionManager }.Execute();
        public static void Create(IConnectionManager connectionManager, string tableName, List<TableColumn> columns) => new CreateTableTask(tableName, columns.Cast<ITableColumn>().ToList()) { ConnectionManager = connectionManager }.Execute();
        public static void Create(IConnectionManager connectionManager, TableDefinition tableDefinition) => new CreateTableTask(tableDefinition) { ConnectionManager = connectionManager }.Execute();

        string ColumnsDefinitionSql => String.Join("  , " + Environment.NewLine, Columns?.Select(col => CreateTableDefinition(col)));

        string CreateTableDefinition(ITableColumn col)
        {
            string dataType = string.Empty;
            if (String.IsNullOrWhiteSpace(col.ComputedColumn))
                dataType = DataTypeConverter.TryGetDBSpecificType(col.DataType, this.ConnectionType);
            string identitySql = col.IsIdentity
                                    ? $"IDENTITY({col.IdentitySeed ?? 1},{col.IdentityIncrement ?? 1})"
                                    : string.Empty;
            string collationSql = !String.IsNullOrWhiteSpace(col.Collation)
                                    ? $"collate {col.Collation}"
                                    : string.Empty;
            string nullSql = string.Empty;
            if (String.IsNullOrWhiteSpace(col.ComputedColumn))
                nullSql = col.AllowNulls
                            ? "NULL"
                            : "NOT NULL";
            string primarySql = col.IsPrimaryKey
                                ? $"CONSTRAINT [pk_{TableWithoutSchema}_{col.Name}] PRIMARY KEY CLUSTERED ( [{col.Name}] ASC )"
                                : string.Empty;
            string defaultSql = string.Empty;
            if (!col.IsPrimaryKey)
                defaultSql = col.DefaultValue != null ? DefaultConstraintName(col.DefaultConstraintName) + $" DEFAULT {SetQuotesIfString(col.DefaultValue)}" : string.Empty;
            string computedColumnSql = !String.IsNullOrWhiteSpace(col.ComputedColumn)
                                        ? $"AS {col.ComputedColumn}"
                                        : string.Empty;
            return $@"[{col.Name}] {dataType} {identitySql} {collationSql} {nullSql} {primarySql} {defaultSql} {computedColumnSql}";
        }

        string DefaultConstraintName(string defConstrName) => !String.IsNullOrWhiteSpace(defConstrName) ? $"CONSTRAINT {defConstrName}" : string.Empty;

        string SetQuotesIfString(string value)
        {
            if (!Regex.IsMatch(value, @"^\d+(\.\d+|)$"))//@" ^ (\d|\.)+$"))
                return $"'{value}'";
            else
                return value;

        }
    }
}
