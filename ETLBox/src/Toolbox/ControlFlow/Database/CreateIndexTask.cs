using System.Linq;
using ALE.ETLBox.Common;
using ALE.ETLBox.Common.ControlFlow;
using ETLBox.Primitives;

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
    [PublicAPI]
    public class CreateIndexTask : GenericTask
    {
        /* ITask Interface */
        public override string TaskName => $"Create index {IndexName} on table {TableName}";

        public void Execute()
        {
            if (
                new IfIndexExistsTask(IndexName, TableName)
                {
                    ConnectionManager = ConnectionManager,
                    DisableLogging = true
                }.Exists()
            )
                new DropIndexTask(IndexName, TableName)
                {
                    ConnectionManager = ConnectionManager,
                    DisableLogging = true
                }.DropIfExists();
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        public void CreateOrRecreate() => Execute();

        /* Public properties */
        public string IndexName { get; set; }
        public ObjectNameDescriptor IN => new(IndexName, QB, QE);
        public string TableName { get; set; }
        public ObjectNameDescriptor TN => new(TableName, QB, QE);
        public IList<string> IndexColumns { get; set; }
        public IList<string> IncludeColumns { get; set; }
        public bool IsUnique { get; set; }
        public bool IsClustered { get; set; }
        public string Sql
        {
            get
            {
                return $@"CREATE {UniqueSql} {ClusteredSql} INDEX {IN.QuotedFullName} ON {TN.QuotedFullName}
( {string.Join(",", IndexColumns.Select(col => QB + col + QE))} )
{IncludeSql}
";
            }
        }

        public CreateIndexTask() { }

        public CreateIndexTask(string indexName, string tableName, IList<string> indexColumns)
            : this()
        {
            IndexName = indexName;
            TableName = tableName;
            IndexColumns = indexColumns;
        }

        public CreateIndexTask(
            string indexName,
            string tableName,
            IList<string> indexColumns,
            IList<string> includeColumns
        )
            : this(indexName, tableName, indexColumns)
        {
            IncludeColumns = includeColumns;
        }

        public static void CreateOrRecreate(
            string indexName,
            string tableName,
            IList<string> indexColumns
        ) => new CreateIndexTask(indexName, tableName, indexColumns).Execute();

        public static void CreateOrRecreate(
            string indexName,
            string tableName,
            IList<string> indexColumns,
            IList<string> includeColumns
        ) => new CreateIndexTask(indexName, tableName, indexColumns, includeColumns).Execute();

        public static void CreateOrRecreate(
            IConnectionManager connectionManager,
            string indexName,
            string tableName,
            IList<string> indexColumns
        ) =>
            new CreateIndexTask(indexName, tableName, indexColumns)
            {
                ConnectionManager = connectionManager
            }.Execute();

        public static void CreateOrRecreate(
            IConnectionManager connectionManager,
            string indexName,
            string tableName,
            IList<string> indexColumns,
            IList<string> includeColumns
        ) =>
            new CreateIndexTask(indexName, tableName, indexColumns, includeColumns)
            {
                ConnectionManager = connectionManager
            }.Execute();

        private string UniqueSql => IsUnique ? "UNIQUE" : string.Empty;

        private string ClusteredSql
        {
            get
            {
                if (ConnectionType == ConnectionManagerType.SqlServer)
                    return IsClustered ? "CLUSTERED" : "NONCLUSTERED";
                return string.Empty;
            }
        }

        private string IncludeSql
        {
            get =>
                IncludeColumns == null
                || IncludeColumns?.Count == 0
                || ConnectionType == ConnectionManagerType.SQLite
                    ? string.Empty
                    : $"INCLUDE ({string.Join("  ,", IncludeColumns!.Select(col => QB + col + QE))})";
        }
    }
}
