using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Drops a view. Use DropIfExists to drop a view only if it exists. 
    /// </summary>
    public class DropViewTask : DropTask<IfTableOrViewExistsTask>, ITask
    {
        /* Implementation */
        internal override string GetSql()
        {
                return $@"DROP VIEW {ON.QuotatedFullName}";
        }

        /* Some constructors */
        public DropViewTask()
        {
        }

        public DropViewTask(string viewName) : this()
        {
            ObjectName = viewName;
        }

        /* Static methods for convenience */
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
