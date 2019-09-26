using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Drops an index if the index exists.
    /// </summary>
    public class DropIndexTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Drop Index {IndexName} on Table {TableName}";
        public override void Execute()
        {
            bool indexExists = new IfIndexExistsTask(IndexName, TableName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (indexExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string IndexName { get; set; }
        public string TableName { get; set; }
        public TableNameDescriptor TN => new TableNameDescriptor(TableName, ConnectionType);
        public string Sql
        {
            get
            {
                string sql = $@"DROP INDEX {QB}{IndexName}{QE}";
                if (ConnectionType != ConnectionManagerType.SQLite && ConnectionType != ConnectionManagerType.Postgres)
                    sql += $@" ON {TN.QuotatedFullName}";
                return sql;
            }

        }

        public void Drop() => Execute();

        /* Some constructors */
        public DropIndexTask()
        {
        }

        public DropIndexTask(string indexName, string tableName) : this()
        {
            TableName = tableName;
            IndexName = indexName;
        }


        /* Static methods for convenience */
        public static void Drop(string indexName, string tableName) => new DropIndexTask(indexName, tableName).Execute();
        public static void Drop(IConnectionManager connectionManager, string indexName, string tableName)
            => new DropIndexTask(indexName, tableName) { ConnectionManager = connectionManager }.Execute();
    }


}
