using ETLBox.Connection;
using ETLBox.Helper;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Drops an index. Use DropIfExists to drop an index only if it exists.
    /// </summary>
    public class DropIndexTask : DropTask<IfIndexExistsTask>, ILoggableTask
    {
        /// <summary>
        /// The table name on which the index is based on.
        /// </summary>
        public string TableName => OnObjectName;

        /// <summary>
        /// The formatted table name on which the index is based on.
        /// </summary>
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);
        internal override string GetSql()
        {
            string sql = $@"DROP INDEX {ON.QuotatedFullName}";
            if (ConnectionType != ConnectionManagerType.SQLite
                && ConnectionType != ConnectionManagerType.Postgres
                && ConnectionType != ConnectionManagerType.Oracle
                )
                sql += $@" ON {TN.QuotatedFullName}";
            return sql;
        }

        public DropIndexTask()
        {
        }

        public DropIndexTask(string indexName, string tableName) : this()
        {
            OnObjectName = tableName;
            ObjectName = indexName;
        }

        /// <summary>
        /// Drops an index.
        /// </summary>
        /// <param name="indexName">The index name to drop.</param>
        /// <param name="tableName">The table name the index is based on.</param>
        public static void Drop(string indexName, string tableName)
            => new DropIndexTask(indexName, tableName).Drop();

        /// <summary>
        /// Drops an index.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="indexName">The index name to drop.</param>
        /// <param name="tableName">The table name the index is based on.</param>
        public static void Drop(IConnectionManager connectionManager, string indexName, string tableName)
            => new DropIndexTask(indexName, tableName) { ConnectionManager = connectionManager }.Drop();

        /// <summary>
        /// Drops an index if the index exists.
        /// </summary>
        /// <param name="indexName">The index name to drop.</param>
        /// <param name="tableName">The table name the index is based on.</param>
        public static void DropIfExists(string indexName, string tableName)
            => new DropIndexTask(indexName, tableName).DropIfExists();

        /// <summary>
        /// Drops an index if the index exists.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="indexName">The index name to drop.</param>
        /// <param name="tableName">The table name the index is based on.</param>
        public static void DropIfExists(IConnectionManager connectionManager, string indexName, string tableName)
            => new DropIndexTask(indexName, tableName) { ConnectionManager = connectionManager }.DropIfExists();
    }


}
