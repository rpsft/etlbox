using ETLBox.Connection;
using ETLBox.Exceptions;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Checks if a database exists.
    /// </summary>
    public class IfDatabaseExistsTask : IfExistsTask
    {
        internal override string GetSql()
        {
            if (!DbConnectionManager.SupportDatabases)
                throw new ETLBoxNotSupportedException("This task is not supported!");

            if (this.ConnectionType == ConnectionManagerType.SqlServer)
            {
                return $@"SELECT COUNT(*) FROM sys.databases WHERE [NAME] = '{ON.UnquotatedObjectName}'";
            }
            else if (this.ConnectionType == ConnectionManagerType.MySql)
            {
                return $@"SELECT COUNT(*)  FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{ON.UnquotatedObjectName}'";
            }
            else if (this.ConnectionType == ConnectionManagerType.Postgres)
            {
                return $@"SELECT COUNT(*) FROM pg_database WHERE datname = '{ON.UnquotatedObjectName}'";
            }
            else
            {
                return string.Empty;
            }
        }
        public IfDatabaseExistsTask()
        {
        }

        public IfDatabaseExistsTask(string databaseName) : this()
        {
            ObjectName = databaseName;
        }


        /// <summary>
        /// Ćhecks if the database exists. Make sure that your default connection string points to the server itself and to an existing database.
        /// (E.g. a system database)
        /// </summary>
        /// <param name="databaseName">The database name that you want to check for existence</param>
        /// <returns>True if the database exists</returns>
        public static bool IsExisting(string databaseName) => new IfDatabaseExistsTask(databaseName).Exists();

        /// <summary>
        /// Ćhecks if the database exists
        /// </summary>
        /// <param name="connectionManager">The connection manager of the server you want to connect. Make sure this points to a database
        /// that does exist (e.g. a system database)</param>
        /// <param name="databaseName">The database name that you want to check for existence</param>
        /// <returns>True if the procedure exists</returns>
        public static bool IsExisting(IConnectionManager connectionManager, string databaseName)
            => new IfDatabaseExistsTask(databaseName) { ConnectionManager = connectionManager }.Exists();
    }
}