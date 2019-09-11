using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Drops a table if the table exists.
    /// </summary>
    public class DropViewTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DROPVIEW";
        public override string TaskName => $"Drop View {ViewName}";
        public override void Execute()
        {
            bool viewExists = new IfExistsTask(ViewName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (viewExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string ViewName { get; set; }
        public string Sql
        {
            get
            {
                return $@"DROP VIEW {ViewName}";
            }
        }

        public void Drop() => Execute();

        /* Some constructors */
        public DropViewTask()
        {
        }

        public DropViewTask(string viewName) : this()
        {
            ViewName = viewName;
        }


        /* Static methods for convenience */
        public static void Drop(string viewName) => new DropViewTask(viewName).Execute();
        public static void Drop(IConnectionManager connectionManager, string viewName) => new DropViewTask(viewName) { ConnectionManager = connectionManager }.Execute();
    }


}
