using ETLBox.Connection;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Drops a view. Use DropIfExists to drop a view only if it exists.
    /// </summary>
    public class DropViewTask : DropTask<IfTableOrViewExistsTask>, ILoggableTask
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

        /// <summary>
        /// Drops a view.
        /// </summary>
        /// <param name="viewName">Name of the view to drop</param>
        public static void Drop(string viewName)
            => new DropViewTask(viewName).Drop();

        /// <summary>
        /// Drops a view
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="viewName">Name of the view to drop</param>
        public static void Drop(IConnectionManager connectionManager, string viewName)
            => new DropViewTask(viewName) { ConnectionManager = connectionManager }.Drop();

        /// <summary>
        /// Drops a view if the view exists.
        /// </summary>
        /// <param name="viewName">Name of the view to drop</param>
        public static void DropIfExists(string viewName)
            => new DropViewTask(viewName).DropIfExists();

        /// <summary>
        /// Drops a view if the view exists.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="viewName">Name of the view to drop</param>
        public static void DropIfExists(IConnectionManager connectionManager, string viewName)
            => new DropViewTask(viewName) { ConnectionManager = connectionManager }.DropIfExists();

    }


}
