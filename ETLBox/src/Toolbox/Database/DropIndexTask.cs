using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Drops an index if the index exists.
    /// </summary>
    public class DropIndexTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DROPINDEX";
        public override string TaskName => $"Drop Index {IndexName} on Table {TableName}";
        public override void Execute()
        {
            bool viewExists = new IfExistsTask(IndexName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (viewExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string IndexName { get; set; }
        public string TableName { get; set; }
        public string Sql
        {
            get
            {
                string sql = $@"DROP INDEX {IndexName}";
                if (ConnectionType != ConnectionManagerType.SQLLite)
                    sql += $@" ON {TableName}";
                return sql;
            }

        }

        public void Drop() => Execute();

        /* Some constructors */
        public DropIndexTask()
        {
        }

        public DropIndexTask(string tableName, string indexName) : this()
        {
            TableName = tableName;
            IndexName = indexName;
        }


        /* Static methods for convenience */
        public static void Drop(string tableName, string indexName) => new DropIndexTask(tableName, indexName).Execute();
        public static void Drop(IConnectionManager connectionManager, string tableName, string indexName)
            => new DropIndexTask(tableName,indexName) { ConnectionManager = connectionManager }.Execute();
    }


}
