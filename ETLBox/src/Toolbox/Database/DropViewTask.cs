using ETLBox.Connection;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Drops a view. Use DropIfExists to drop a view only if it exists.
    /// </summary>
    public class DropViewTask : DropTask<IfTableOrViewExistsTask>, ITask
    {
        internal override string GetSql()
        {
            return $@"DROP VIEW { ON.QuotatedFullName }";
        }

        public DropViewTask()
        {
        }

        public DropViewTask(string viewName) : this()
        {
            ObjectName = viewName;
        }

        public static void Drop(string viewName)
            => new DropViewTask(viewName).Drop();
        public static void Drop(IConnectionManager connectionManager, string viewName)
            => new DropViewTask(viewName) { ConnectionManager = connectionManager }.Drop();
        public static void DropIfExists(string viewName)
            => new DropViewTask(viewName).DropIfExists();
        public static void DropIfExists(IConnectionManager connectionManager, string viewName)
            => new DropViewTask(viewName) { ConnectionManager = connectionManager }.DropIfExists();

    }


}
