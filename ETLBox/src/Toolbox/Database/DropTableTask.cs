using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Drops a table if the table exists.
    /// </summary>
    public class DropTableTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DROPTABLE";
        public override string TaskName => $"Drop Table {TableName}";
        public override void Execute()
        {
            bool tableExists = new IfExistsTask(TableName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (tableExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string TableName { get; set; }
        public string Sql
        {
            get
            {
                return $@"DROP TABLE {TableName}";
            }
        }

        public void Drop() => Execute();

        /* Some constructors */
        public DropTableTask()
        {
        }

        public DropTableTask(string tableName) : this()
        {
            TableName = tableName;
        }


        /* Static methods for convenience */
        public static void Drop(string tableName) => new DropTableTask(tableName).Execute();
        public static void Drop(IConnectionManager connectionManager, string tableName) => new DropTableTask(tableName) { ConnectionManager = connectionManager }.Execute();


    }


}
