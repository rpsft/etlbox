using ETLBox.Connection;
using ETLBox.Helper;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Truncates a table.
    /// </summary>
    /// <example>
    /// <code>
    /// TruncateTableTask.Truncate("demo.table1");
    /// </code>
    /// </example>
    public class TruncateTableTask : ControlFlowTask
    {
        /// <inheritdoc/>
        public override string TaskName => $"Truncate table {TableName}";

        /// <summary>
        /// Executes the table truncation.
        /// </summary>
        public void Execute()
        {
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /// <summary>
        /// Name of the table that should be truncated
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// The formatted table table name
        /// </summary>
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);

        /// <summary>
        /// Sql code that is used when the task is executed.
        /// </summary>
        public string Sql
        {
            get
            {
                if (ConnectionType == ConnectionManagerType.SQLite
                    || ConnectionType == ConnectionManagerType.Access)
                    return $@"DELETE FROM {TN.QuotatedFullName}";
                else
                    return $@"TRUNCATE TABLE {TN.QuotatedFullName}";
            }
        }

        public TruncateTableTask()
        {

        }

        /// <param name="tableName">Sets the <see cref="TableName"/></param>
        public TruncateTableTask(string tableName) : this()
        {
            this.TableName = tableName;
        }

        /// <summary>
        /// Execute a table truncation
        /// </summary>
        /// <param name="tableName">Table that should be truncated</param>
        public static void Truncate(string tableName) => new TruncateTableTask(tableName).Execute();

        /// <summary>
        /// Execute a table truncation
        /// </summary>
        /// <param name="tableName">Table name that should be truncated</param>
        /// <param name="connection">Database connection manager to connect with the database</param>
        public static void Truncate(IConnectionManager connection, string tableName) => new TruncateTableTask(tableName) { ConnectionManager = connection }.Execute();
    }
}
