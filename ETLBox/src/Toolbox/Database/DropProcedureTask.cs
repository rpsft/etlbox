using ETLBox.Connection;
using ETLBox.Exceptions;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Drops a procedure. Use DropIfExists to drop a procedure only if it exists.
    /// </summary>
    public class DropProcedureTask : DropTask<IfProcedureExistsTask>, ILoggableTask
    {
        internal override string GetSql()
        {
            if (!DbConnectionManager.SupportProcedures)
                throw new ETLBoxNotSupportedException("This task is not supported!");

            return $@"DROP PROCEDURE {ON.QuotatedFullName}";
        }

        public DropProcedureTask()
        {
        }

        public DropProcedureTask(string procedureName) : this()
        {
            ObjectName = procedureName;
        }

        /// <summary>
        /// Drops a procedure.
        /// </summary>
        /// <param name="procedureName">Name of the procedure to drop</param>
        public static void Drop(string procedureName)
            => new DropProcedureTask(procedureName).Drop();

        /// <summary>
        /// Drops a procedure.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="procedureName">Name of the procedure to drop</param>
        public static void Drop(IConnectionManager connectionManager, string procedureName)
            => new DropProcedureTask(procedureName) { ConnectionManager = connectionManager }.Drop();

        /// <summary>
        /// Drops a procedure if the procedure exists.
        /// </summary>
        /// <param name="procedureName">Name of the procedure to drop</param>
        public static void DropIfExists(string procedureName)
            => new DropProcedureTask(procedureName).DropIfExists();

        /// <summary>
        /// Drops a procedure if the procedure exists.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="procedureName">Name of the procedure to drop</param>
        public static void DropIfExists(IConnectionManager connectionManager, string procedureName)
            => new DropProcedureTask(procedureName) { ConnectionManager = connectionManager }.DropIfExists();
    }
}
