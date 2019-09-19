using ALE.ETLBox.ConnectionManager;
using System;


namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Count the row in a table. This task normally uses the  COUNT(*) method (could take some time on big tables)
    /// For Sql Server, you can set the QuickQueryMode to true. This will query the sys.partition table which can be much faster.
    /// NoLock does a normal COUNT(*) using the nolock - option which avoid tables locks when reading from the table (but while counting the tables
    /// new data could be inserted, which could lead to wrong results).
    /// </summary>
    /// <example>
    /// <code>
    /// int count = RowCountTask.Count("tableName").Value;
    /// </code>
    /// </example>
    public class RowCountTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "ROWCOUNT";
        public override string TaskName => $"Count Rows for {TableName}" + (HasCondition ? $" with condition {Condition}" : "");
        public override void Execute()
        {
            if (this.ConnectionType == ETLBox.ConnectionManager.ConnectionManagerType.SqlServer
                || this.ConnectionType == ETLBox.ConnectionManager.ConnectionManagerType.Access)
                Rows = new SqlTask(this, Sql).ExecuteScalar<int>();
            else
                Rows = (int)new SqlTask(this, Sql).ExecuteScalar<long>();
        }

        public string TableName { get; set; }
        public string Condition { get; set; }
        public bool HasCondition => !String.IsNullOrWhiteSpace(Condition);
        public int? Rows { get; private set; }
        public bool? HasRows => Rows > 0;
        public bool QuickQueryMode { get; set; }
        public bool NoLock { get; set; }
        public string Sql
        {
            get
            {
                return QuickQueryMode && !HasCondition ? $@"
SELECT CAST ( SUM ( [rows]) AS INT) 
FROM [sys].[partitions] 
WHERE [object_id] = object_id(N'{TableName}') 
  AND [index_id] IN (0,1)" :
                $@"
SELECT COUNT(*)
FROM {TableName} 
{WhereClause} {Condition} {NoLockHint}";
            }
        }

        public RowCountTask() { }

        public RowCountTask(string tableName)
        {
            this.TableName = tableName;
        }

        public RowCountTask(string tableName, RowCountOptions options) : this(tableName)
        {
            if (options == RowCountOptions.QuickQueryMode)
                QuickQueryMode = true;
            if (options == RowCountOptions.NoLock)
                NoLock = true;

        }

        public RowCountTask(string tableName, string condition) : this(tableName)
        {
            this.Condition = condition;
        }


        public RowCountTask(string tableName, string condition, RowCountOptions options) : this(tableName, options)
        {
            this.Condition = condition;
        }

        public RowCountTask Count()
        {
            Execute();
            return this;
        }

        public static int? Count(string tableName) => new RowCountTask(tableName).Count().Rows;
        public static int? Count(string tableName, RowCountOptions options) => new RowCountTask(tableName, options).Count().Rows;
        public static int? Count(string tableName, string condition) => new RowCountTask(tableName, condition).Count().Rows;
        public static int? Count(string tableName, string condition, RowCountOptions options) => new RowCountTask(tableName, condition, options).Count().Rows;
        public static int? Count(IConnectionManager connectionManager, string tableName) => new RowCountTask(tableName) { ConnectionManager = connectionManager }.Count().Rows;
        public static int? Count(IConnectionManager connectionManager, string tableName, RowCountOptions options) => new RowCountTask(tableName, options) { ConnectionManager = connectionManager }.Count().Rows;
        public static int? Count(IConnectionManager connectionManager, string tableName, string condition) => new RowCountTask(tableName, condition) { ConnectionManager = connectionManager }.Count().Rows;
        public static int? Count(IConnectionManager connectionManager, string tableName, string condition, RowCountOptions options) => new RowCountTask(tableName, condition, options) { ConnectionManager = connectionManager }.Count().Rows;

        string WhereClause => HasCondition ? "WHERE" : String.Empty;
        string NoLockHint => NoLock ? "WITH (NOLOCK)" : String.Empty;

    }

    /// <summary>
    /// Used in the RowCountTask. None forces the RowCountTask to do a normal COUNT(*) and works on all databases.
    /// QuickQueryMode only works on SqlServer and uses the partition table which can be much faster on tables with a big amount of data.
    /// NoLock does a normal COUNT(*) using the nolock - option which avoid tables locks when reading from the table (but while counting the tables
    /// new data could be inserted, which could lead to wrong results).
    /// </summary>
    public enum RowCountOptions
    {
        None,
        QuickQueryMode,
        NoLock
    }
}
