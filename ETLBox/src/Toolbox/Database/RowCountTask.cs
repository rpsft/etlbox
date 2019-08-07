using System;


namespace ALE.ETLBox.ControlFlow {
    /// <summary>
    /// Count the row in a table. This task normally uses the  COUNT(*) method (could take some time on big tables)
    /// For Sql Server, you can set the QuickQueryMode to true. This will query the sys.partition table which can be much faster.
    /// </summary>
    /// <example>
    /// <code>
    /// int count = RowCountTask.Count("table1").Value;
    /// </code>
    /// </example>
    public class RowCountTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "ROWCOUNT";
        public override string TaskName => $"Count Rows for {TableName}" + (HasCondition ? $" with condition {Condition}" : "");
        public override void Execute() {
            if (this.ConnectionType == ETLBox.ConnectionManager.ConnectionManagerType.SQLLite)
                Rows = (int)new SqlTask(this, Sql).ExecuteScalar<long>();
            else
                Rows = new SqlTask(this, Sql).ExecuteScalar<int>();
        }

        public string TableName { get; set; }
        public string Condition { get; set; }
        public bool HasCondition => !String.IsNullOrWhiteSpace(Condition);
        public int? Rows { get; private set; }
        public bool? HasRows => Rows > 0;
        public bool QuickQueryMode { get; set; }
        public bool NoLock { get; set; }
        public string Sql {
            get {
                return QuickQueryMode && !HasCondition ? $@"
SELECT CAST ( SUM ( [rows]) AS INT) 
FROM [sys].[partitions] 
WHERE [object_id] = object_id(N'{TableName}') 
  AND [index_id] IN (0,1)" :
                $@"
SELECT COUNT (*)
FROM {TableName} 
{WhereClause} {Condition} {NoLockHint}";
            }
        }

        public RowCountTask() {

        }
        public RowCountTask(string tableName) {
            this.TableName = tableName;
        }

        public RowCountTask(string tableName, RowCountOptions options) : this(tableName) {
            if (options == RowCountOptions.QuickQueryMode)
                QuickQueryMode = true;
            if (options == RowCountOptions.NoLock)
                NoLock = true;

        }

        public RowCountTask(string tableName, string condition) : this(tableName) {
            this.Condition = condition;
        }

        public RowCountTask(string tableName, string condition, RowCountOptions options) : this(tableName,options) {
            this.Condition = condition;
        }

        public RowCountTask Count() {
            Execute();
            return this;
        }

        public static int? Count(string tableName) => new RowCountTask(tableName).Count().Rows;
        public static int? Count(string tableName, RowCountOptions options) => new RowCountTask(tableName,options).Count().Rows;
        public static int? Count(string tableName, string condition) => new RowCountTask(tableName, condition).Count().Rows;
        public static int? Count(string tableName, string condition, RowCountOptions options) => new RowCountTask(tableName, condition, options).Count().Rows;


        string WhereClause => HasCondition ? "WHERE" : String.Empty;
        string NoLockHint => NoLock ? "WITH (NOLOCK)" : String.Empty;

    }

    public enum RowCountOptions {
        None,
        QuickQueryMode,
        NoLock
    }
}
