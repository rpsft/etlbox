using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Drops an index. Use DropIfExists to drop an index only if it exists.
    /// </summary>
    public class DropIndexTask : DropTask<IfIndexExistsTask>, ITask
    {
        public string TableName => OnObjectName;
        public TableNameDescriptor TN => new TableNameDescriptor(TableName, ConnectionType);
        internal override string GetSql()
        {
            string sql = $@"DROP INDEX {ON.QuotatedFullName}";
            if (ConnectionType != ConnectionManagerType.SQLite && ConnectionType != ConnectionManagerType.Postgres)
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

        public static void Drop(string indexName, string tableName)
            => new DropIndexTask(indexName, tableName).Drop();
        public static void Drop(IConnectionManager connectionManager, string indexName, string tableName)
            => new DropIndexTask(indexName, tableName) { ConnectionManager = connectionManager }.Drop();
        public static void DropIfExists(string indexName, string tableName)
            => new DropIndexTask(indexName, tableName).DropIfExists();
        public static void DropIfExists(IConnectionManager connectionManager, string indexName, string tableName)
            => new DropIndexTask(indexName, tableName) { ConnectionManager = connectionManager }.DropIfExists();
    }


}
