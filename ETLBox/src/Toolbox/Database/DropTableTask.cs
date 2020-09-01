using ETLBox.Connection;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Drops a table. Use DropIfExists to drop a table only if it exists.
    /// </summary>
    public class DropTableTask : DropTask<IfTableOrViewExistsTask>, ILoggableTask
    {
        internal override string GetSql()
        {
            return $@"DROP TABLE {ON.QuotatedFullName}";
        }

        public DropTableTask()
        {
        }

        public DropTableTask(string tableName) : this()
        {
            ObjectName = tableName;
        }

        /// <summary>
        /// Drops a table.
        /// </summary>
        /// <param name="tableName">Name of the table to drop</param>
        public static void Drop(string tableName)
            => new DropTableTask(tableName).Drop();

        /// <summary>
        /// Drops a table.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableName">Name of the table to drop</param>
        public static void Drop(IConnectionManager connectionManager, string tableName)
            => new DropTableTask(tableName) { ConnectionManager = connectionManager }.Drop();

        /// <summary>
        /// Drops a table if the table exists.
        /// </summary>
        /// <param name="tableName">Name of the table to drop</param>
        public static void DropIfExists(string tableName)
            => new DropTableTask(tableName).DropIfExists();

        /// <summary>
        /// Drops a table if the table exists.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="tableName">Name of the table to drop</param>
        public static void DropIfExists(IConnectionManager connectionManager, string tableName)
            => new DropTableTask(tableName) { ConnectionManager = connectionManager }.DropIfExists();

    }


}
