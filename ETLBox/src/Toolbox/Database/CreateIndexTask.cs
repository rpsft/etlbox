using ETLBox.Connection;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Creates an index if the index doesn't exists, otherwise the index is dropped and recreated.
    /// </summary>
    /// <example>
    /// <code>
    /// CreateIndexTask.Create("indexname","tablename", indexColumns)
    /// </code>
    /// </example>
    public class CreateIndexTask : ControlFlowTask
    {
        /// <inheritdoc />
        public override string TaskName => $"Create index {IndexName} on table {TableName}";

        /// <summary>
        /// Runs the sql to (re)create the index on a table.
        /// </summary>
        public void Execute()
        {
            if (new IfIndexExistsTask(IndexName, TableName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists())
                new DropIndexTask(IndexName, TableName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.DropIfExists();
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /// <summary>
        /// Runs the sql to (re)create the index on a table.
        /// </summary>
        public void CreateOrRecrate() => Execute();

        /// <summary>
        /// The name of the index
        /// </summary>
        public string IndexName { get; set; }

        /// <summary>
        /// The formatted name of the index
        /// </summary>
        public ObjectNameDescriptor IN => new ObjectNameDescriptor(IndexName, QB, QE);

        /// <summary>
        /// The name of the table the index is based on.
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// The formatted name of the table.
        /// </summary>
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);

        /// <summary>
        /// A list of column names for the index
        /// </summary>
        public IList<string> IndexColumns { get; set; }

        /// <summary>
        /// A list of included column names for the index.
        /// </summary>
        public IList<string> IncludeColumns { get; set; }

        /// <summary>
        /// Set the index as Unique.
        /// </summary>
        public bool IsUnique { get; set; }

        /// <summary>
        /// Set the index as a clustered index
        /// </summary>
        public bool IsClustered { get; set; }

        /// <summary>
        /// The sql code used to generate the index
        /// </summary>
        public string Sql
        {
            get
            {
                return $@"CREATE {UniqueSql} {ClusteredSql} INDEX {IN.QuotatedFullName} ON {TN.QuotatedFullName}
( {String.Join(",", IndexColumns.Select(col => QB + col + QE))} )
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

        /// <summary>
        /// Creates an index. If the index exists, it is dropped and recreated.
        /// </summary>
        /// <param name="indexName">The name of the index</param>
        /// <param name="tableName">The name of the table the index is based on</param>
        /// <param name="indexColumns">The name of the columns for the index</param>
        public static void CreateOrRecreate(string indexName, string tableName, IList<string> indexColumns)
            => new CreateIndexTask(indexName, tableName, indexColumns).Execute();

        /// <summary>
        /// Creates an index. If the index exists, it is dropped and recreated.
        /// </summary>
        /// <param name="indexName">The name of the index</param>
        /// <param name="tableName">The name of the table the index is based on</param>
        /// <param name="indexColumns">The name of the columns for the index</param>
        /// <param name="includeColumns">The name of the columns that are included in the index</param>
        public static void CreateOrRecreate(string indexName, string tableName, IList<string> indexColumns, IList<string> includeColumns)
            => new CreateIndexTask(indexName, tableName, indexColumns, includeColumns).Execute();

        /// <summary>
        /// Creates an index. If the index exists, it is dropped and recreated.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="indexName">The name of the index</param>
        /// <param name="tableName">The name of the table the index is based on</param>
        /// <param name="indexColumns">The name of the columns for the index</param>
        public static void CreateOrRecreate(IConnectionManager connectionManager, string indexName, string tableName, IList<string> indexColumns)
            => new CreateIndexTask(indexName, tableName, indexColumns) { ConnectionManager = connectionManager }.Execute();

        /// <summary>
        /// Creates an index. If the index exists, it is dropped and recreated.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="indexName">The name of the index</param>
        /// <param name="tableName">The name of the table the index is based on</param>
        /// <param name="indexColumns">The name of the columns for the index</param>
        /// <param name="includeColumns">The name of the columns that are included in the index.</param>
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
                    return $"INCLUDE ({String.Join("  ,", IncludeColumns.Select(col => QB + col + QE))})";
            }
        }

    }
}
