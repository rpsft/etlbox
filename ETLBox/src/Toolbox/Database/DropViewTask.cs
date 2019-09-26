using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Drops a view if the view exists.
    /// </summary>
    public class DropViewTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Drop View {ViewName}";
        public override void Execute()
        {
            bool viewExists = new IfTableOrViewExistsTask(ViewName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (viewExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string ViewName { get; set; }
        public TableNameDescriptor TN => new TableNameDescriptor(ViewName, ConnectionType);
        public string Sql
        {
            get
            {
                return $@"DROP VIEW {TN.QuotatedFullName}";
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
