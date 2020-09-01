using ETLBox.Connection;
using ETLBox.Exceptions;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Drops a database. Use DropIfExists to drop a database only if it exists. In MySql, this will drop a schema.
    /// </summary>
    /// <example>
    /// <code>
    /// DropDatabaseTask.Delete("DemoDB");
    /// </code>
    /// </example>
    public class DropDatabaseTask : DropTask<IfDatabaseExistsTask>
    {
        internal override string GetSql()
        {
            if (!DbConnectionManager.SupportDatabases)
                throw new ETLBoxNotSupportedException("This task is not supported!");

            if (ConnectionType == ConnectionManagerType.SqlServer)
            {
                return
$@"
USE [master]
ALTER DATABASE [{ObjectName}]
SET SINGLE_USER WITH ROLLBACK IMMEDIATE
ALTER DATABASE [{ObjectName}]
SET MULTI_USER
DROP DATABASE [{ObjectName}]  
";
            }
            else
            {
                return $@"DROP DATABASE {ON.QuotatedObjectName}";
            }
        }

        public DropDatabaseTask()
        {
        }

        public DropDatabaseTask(string databaseName) : this()
        {
            ObjectName = databaseName;
        }

        /// <summary>
        /// Drops a database. In MySql, this will drop a schema.
        /// Make sure that your default connection string points to the server itself and to an existing database (e.g. a system database).
        /// </summary>
        /// <param name="databaseName">Name of the database (MySql: schema) to drop</param>
        public static void Drop(string databaseName)
            => new DropDatabaseTask(databaseName).Drop();

        /// <summary>
        /// Drops a database. In MySql, this will drop a schema.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the server you want to connect. Make sure this points to a database
        /// that does exist (e.g. a system database)</param>
        /// <param name="databaseName">Name of the database (MySql: schema) to drop</param>
        public static void Drop(IConnectionManager connectionManager, string databaseName)
            => new DropDatabaseTask(databaseName) { ConnectionManager = connectionManager }.Drop();

        /// <summary>
        /// Drops a database if the database exists. In MySql, this will drop a schema.
        /// </summary>
        /// <param name="databaseName">Name of the database (MySql: schema) to drop</param>
        public static void DropIfExists(string databaseName)
            => new DropDatabaseTask(databaseName).DropIfExists();

        /// <summary>
        /// Drops a database if the database exists. In MySql, this will drop a schema.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the server you want to connect. Make sure this points to a database
        /// that does exist (e.g. a system database)</param>
        /// <param name="databaseName">Name of the database (MySql: schema) to drop</param>
        public static void DropIfExists(IConnectionManager connectionManager, string databaseName)
            => new DropDatabaseTask(databaseName) { ConnectionManager = connectionManager }.DropIfExists();
    }
}
