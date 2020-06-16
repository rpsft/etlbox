using ETLBox.Connection;
using ETLBox.Helper;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Drops an index. Use DropIfExists to drop an index only if it exists.
    /// </summary>
    public class DropIndexTask : DropTask<IfIndexExistsTask>, ITask
    {
        public string TableName => OnObjectName;
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
