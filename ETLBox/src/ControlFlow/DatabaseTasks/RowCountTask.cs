using ETLBox.Connection;
using ETLBox.Helper;
using System;


namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Count the row in a table. This task normally uses the  COUNT(*) method (could take some time on big tables).
    /// You can pass a a filter condition for the count.
    /// </summary>
    /// <example>
    /// <code>
    /// int count = RowCountTask.Count("tableName").Value;
    /// </code>
    /// </example>
    public sealed class RowCountTask : ControlFlowTask
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = $"Count Rows for table";

        /// <summary>
        /// Name of the table on which the rows are counted
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// The formatted table table name
        /// </summary>
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);

        /// <summary>
        /// Part of the sql where condition which restrict which rows are counted
        /// </summary>
        public string Condition { get; set; }

        /// <summary>
        /// Will hold the number of counted rows after execution
        /// </summary>
        public int? Rows { get; private set; }

        /// <summary>
        /// Indicates if the table contains rows - only has a value after the execution
        /// </summary>
        public bool? HasAnyRows => Rows > 0;

        /// <summary>
        /// For Sql Server, you can set the QuickQueryMode to true. This will query the sys.partition table which can be much faster.
        /// </summary>
        public bool QuickQueryMode { get; set; }

        /// <summary>
        /// Will do the row count also on uncommitted reads.
        /// </summary>
        public bool DirtyRead { get; set; }

        /// <summary>
        /// The sql that is executed to count the rows in the table - will change depending on your parameters.
        /// </summary>
        public string Sql
        {
            get
            {
                return QuickQueryMode && !HasCondition ? $@"
SELECT SUM ([rows]) 
FROM [sys].[partitions] 
WHERE [object_id] = object_id(N'{TableName}') 
  AND [index_id] IN (0,1)" :
                $@"{MYSQLREADUNCOMMITTED}
SELECT {COUNT}
FROM {TN.QuotatedFullName} {NOLOCK} 
{WhereClause} {LIMIT}
{MYSQLCOMMIT}";
            }
        }

        #endregion

        #region Constructors

        public RowCountTask() { }

        public RowCountTask(string tableName)
        {
            this.TableName = tableName;
        }

        public RowCountTask(string tableName, RowCountOptions options) : this(tableName)
        {
            if (options == RowCountOptions.QuickQueryMode)
                QuickQueryMode = true;
            if (options == RowCountOptions.DirtyRead)
                DirtyRead = true;

        }

        public RowCountTask(string tableName, string condition) : this(tableName)
        {
            this.Condition = condition;
        }


        public RowCountTask(string tableName, string condition, RowCountOptions options) : this(tableName, options)
        {
            this.Condition = condition;
        }

        #endregion

        #region Implementation

        /// <summary>
        /// Performs the row count
        /// </summary>
        public RowCountTask Count()
        {
            Execute();
            return this;
        }

        /// <summary>
        /// Checks if the table has at least one (matching) row.
        /// </summary>
        public RowCountTask HasRows()
        {
            OnlyFirstRow = true;
            Execute();
            return this;
        }

        bool OnlyFirstRow;
        internal void Execute()
        {
            if (DirtyRead && (
                    ConnectionType == ConnectionManagerType.Postgres ||
                    ConnectionType == ConnectionManagerType.Oracle ||
                    ConnectionType == ConnectionManagerType.SQLite
                ))
                DirtyRead = false;
            Rows = new SqlTask(this, Sql).ExecuteScalar<int>();
        }

        bool HasCondition => !String.IsNullOrWhiteSpace(Condition);
        string WhereClause => HasCondition ? $"WHERE ({Condition})" : String.Empty;

        string MYSQLREADUNCOMMITTED
            => (DirtyRead && ConnectionType == ConnectionManagerType.MySql) ?
                $"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;" : string.Empty;

        string MYSQLCOMMIT => (DirtyRead && ConnectionType == ConnectionManagerType.MySql) ?
            $"COMMIT;" : String.Empty;

        string NOLOCK
        {
            get
            {
                if (DirtyRead && ConnectionType == ConnectionManagerType.SqlServer)
                    return "WITH (nolock)";
                else if (DirtyRead && ConnectionType == ConnectionManagerType.Db2)
                    return $"WITH UR";
                else return string.Empty;
            }
        }

        string COUNT
        {
            get
            {
                if (OnlyFirstRow)
                {
                    string sql = "1";
                    if (this.ConnectionType == ConnectionManagerType.SqlServer)
                        return sql = "TOP 1 " + sql;
                    return sql;
                }
                else
                    return "COUNT(*)";
            }
        }

        string LIMIT
        {
            get
            {
                if (OnlyFirstRow)
                {
                    if (this.ConnectionType == ConnectionManagerType.SqlServer)
                        return string.Empty;
                    else if (this.ConnectionType == ConnectionManagerType.Oracle)
                    {
                        if (HasCondition)
                            return "AND rownum = 1";
                        else
                            return "WHERE rownum = 1";
                    }
                    else
                        return "LIMIT 1";
                }
                else
                    return string.Empty;
            }
        }


        #endregion

        #region Static convenience methods

        public static int Count(string tableName) => new RowCountTask(tableName).Count().Rows ?? 0;
        public static int Count(string tableName, RowCountOptions options) => new RowCountTask(tableName, options).Count().Rows ?? 0;
        public static int Count(string tableName, string condition) => new RowCountTask(tableName, condition).Count().Rows ?? 0;
        public static int Count(string tableName, string condition, RowCountOptions options) => new RowCountTask(tableName, condition, options).Count().Rows ?? 0;
        public static int Count(IConnectionManager connectionManager, string tableName) => new RowCountTask(tableName) { ConnectionManager = connectionManager }.Count().Rows ?? 0;
        public static int Count(IConnectionManager connectionManager, string tableName, RowCountOptions options) => new RowCountTask(tableName, options) { ConnectionManager = connectionManager }.Count().Rows ?? 0;
        public static int Count(IConnectionManager connectionManager, string tableName, string condition) => new RowCountTask(tableName, condition) { ConnectionManager = connectionManager }.Count().Rows ?? 0;
        public static int Count(IConnectionManager connectionManager, string tableName, string condition, RowCountOptions options) => new RowCountTask(tableName, condition, options) { ConnectionManager = connectionManager }.Count().Rows ?? 0;

        public static bool HasRows(string tableName) => new RowCountTask(tableName).HasRows().HasAnyRows ?? false;
        public static bool HasRows(string tableName, RowCountOptions options) => new RowCountTask(tableName, options).HasRows().HasAnyRows ?? false;
        public static bool HasRows(string tableName, string condition) => new RowCountTask(tableName, condition).HasRows().HasAnyRows ?? false;
        public static bool HasRows(string tableName, string condition, RowCountOptions options) => new RowCountTask(tableName, condition, options).HasRows().HasAnyRows ?? false;
        public static bool HasRows(IConnectionManager connectionManager, string tableName) => new RowCountTask(tableName) { ConnectionManager = connectionManager }.HasRows().HasAnyRows ?? false;
        public static bool HasRows(IConnectionManager connectionManager, string tableName, RowCountOptions options) => new RowCountTask(tableName, options) { ConnectionManager = connectionManager }.HasRows().HasAnyRows ?? false;
        public static bool HasRows(IConnectionManager connectionManager, string tableName, string condition) => new RowCountTask(tableName, condition) { ConnectionManager = connectionManager }.HasRows().HasAnyRows ?? false;
        public static bool HasRows(IConnectionManager connectionManager, string tableName, string condition, RowCountOptions options) => new RowCountTask(tableName, condition, options) { ConnectionManager = connectionManager }.HasRows().HasAnyRows ?? false;

        #endregion
    }

    /// <summary>
    /// Used in the RowCountTask. None forces the RowCountTask to do a normal COUNT(*) and works on all databases.
    /// QuickQueryMode only works on SqlServer and uses the partition table which can be much faster on tables with a big amount of data.
    /// DirtyRead does a normal COUNT(*) but also reading uncommitted reads. 
    /// </summary>
    public enum RowCountOptions
    {
        None,
        QuickQueryMode,
        DirtyRead
    }
}
