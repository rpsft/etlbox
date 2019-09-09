using ALE.ETLBox.ConnectionManager;
using System;
using System.Collections.Generic;

namespace ALE.ETLBox.ControlFlow {
    /// <summary>
    /// Creates an index if the index doesn't exists.
    /// </summary>
    /// <example>
    /// <code>
    /// CreateIndexTask.Create("indexname","tablename", indexColumns)
    /// </code>
    /// </example>
    public class CreateIndexTask: GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CREATEINDEX";
        public override string TaskName => $"Create index {IndexName} on table {TableName}";
        public override void Execute() => new SqlTask(this, Sql).ExecuteNonQuery();

        /* Public properties */
        public string IndexName { get; set; }
        public string TableName { get; set; }
        public IList<string> IndexColumns { get; set; }
        public IList<string> IncludeColumns { get; set; }
        public bool IsUnique { get; set; }
        public bool IsClustered { get; set; }
        public string Sql => $@"
IF NOT EXISTS (SELECT *  FROM sys.indexes  WHERE name='{IndexName}' AND object_id = object_id('{TableName}'))
  CREATE {UniqueSql} {ClusteredSql} INDEX {IndexName} ON {TableName}
  ( {String.Join(",", IndexColumns)} )
  {IncludeSql}
  WITH(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = ON, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
";

        public CreateIndexTask() {

        }
        public CreateIndexTask(string indexName, string tableName, IList<string> indexColumns) : this() {
            this.IndexName = indexName;
            this.TableName = tableName;
            this.IndexColumns = indexColumns;
        }

        public CreateIndexTask(string indexName, string tableName, IList<string> indexColumns, IList<string> includeColumns) : this(indexName, tableName, indexColumns) {
            this.IncludeColumns = includeColumns;
        }
        public static void Create(string indexName, string tableName, IList<string> indexColumns)
            => new CreateIndexTask(indexName,tableName,indexColumns).Execute();
        public static void Create(string indexName, string tableName, IList<string> indexColumns, IList<string> includeColumns)
            => new CreateIndexTask(indexName, tableName, indexColumns, includeColumns).Execute();
        public static void Create(IConnectionManager connectionManager, string indexName, string tableName, IList<string> indexColumns)
            => new CreateIndexTask(indexName, tableName, indexColumns) { ConnectionManager = connectionManager }.Execute();
        public static void Create(IConnectionManager connectionManager, string indexName, string tableName, IList<string> indexColumns, IList<string> includeColumns)
            => new CreateIndexTask(indexName, tableName, indexColumns, includeColumns) { ConnectionManager = connectionManager}.Execute();

        string UniqueSql => IsUnique ? "UNIQUE" : string.Empty;
        string ClusteredSql => IsClustered ? "CLUSTERED" : "NONCLUSTERED";
        string IncludeSql => IncludeColumns?.Count > 0 ? $"INCLUDE ({String.Join("  ,", IncludeColumns)})" : string.Empty;

    }
}
