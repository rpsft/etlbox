using ALE.ETLBox.ConnectionManager;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Creates an index if the index doesn't exists, otherwise the index is dropped and recreated.
    /// </summary>
    /// <example>
    /// <code>
    /// CreateIndexTask.Create("indexname","tablename", indexColumns)
    /// </code>
    /// </example>
    public class CreateIndexTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Create index {IndexName} on table {TableName}";
        public override void Execute()
        {
            if (new IfIndexExistsTask(IndexName, TableName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists())
                new DropIndexTask(IndexName, TableName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Drop();
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        public void CreateOrRecrate() => Execute();

        /* Public properties */
        public string IndexName { get; set; }
        public TableNameDescriptor IN => new TableNameDescriptor(IndexName, ConnectionType);
        public string TableName { get; set; }
        public TableNameDescriptor TN => new TableNameDescriptor(TableName, ConnectionType);
        public IList<string> IndexColumns { get; set; }
        public IList<string> IncludeColumns { get; set; }
        public bool IsUnique { get; set; }
        public bool IsClustered { get; set; }
        public string Sql
        {
            get
            {
                return $@"CREATE {UniqueSql} {ClusteredSql} INDEX {IN.QuotatedFullName} ON {TN.QuotatedFullName}
( {String.Join(",", IndexColumns.Select(col => QB + col+ QE))} )
{IncludeSql}
";
            }
        }

        public CreateIndexTask()
        {

        }
        public CreateIndexTask(string indexName, string tableName, IList<string> indexColumns) : this()
        {
            this.IndexName = indexName;
            this.TableName = tableName;
            this.IndexColumns = indexColumns;
        }

        public CreateIndexTask(string indexName, string tableName, IList<string> indexColumns, IList<string> includeColumns) : this(indexName, tableName, indexColumns)
        {
            this.IncludeColumns = includeColumns;
        }
        public static void CreateOrRecreate(string indexName, string tableName, IList<string> indexColumns)
            => new CreateIndexTask(indexName, tableName, indexColumns).Execute();
        public static void CreateOrRecreate(string indexName, string tableName, IList<string> indexColumns, IList<string> includeColumns)
            => new CreateIndexTask(indexName, tableName, indexColumns, includeColumns).Execute();
        public static void CreateOrRecreate(IConnectionManager connectionManager, string indexName, string tableName, IList<string> indexColumns)
            => new CreateIndexTask(indexName, tableName, indexColumns) { ConnectionManager = connectionManager }.Execute();
        public static void CreateOrRecreate(IConnectionManager connectionManager, string indexName, string tableName, IList<string> indexColumns, IList<string> includeColumns)
            => new CreateIndexTask(indexName, tableName, indexColumns, includeColumns) { ConnectionManager = connectionManager }.Execute();

        string UniqueSql => IsUnique ? "UNIQUE" : string.Empty;
        string ClusteredSql
        {
            get
            {
                if (ConnectionType == ConnectionManagerType.SqlServer)
                    return IsClustered ? "CLUSTERED" : "NONCLUSTERED";
                else
                    return string.Empty;
            }
        }
        string IncludeSql
        {
            get
            {
                if (IncludeColumns == null
                    || IncludeColumns?.Count == 0
                    || ConnectionType == ConnectionManagerType.SQLite)
                    return string.Empty;
                else
                    return $"INCLUDE ({String.Join("  ,", IncludeColumns.Select ( col => QB+col+QE))})";
            }
        }

    }
}
