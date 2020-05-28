using ETLBox.ConnectionManager;

namespace ETLBox.ControlFlow
{
    /// <summary>
    /// Checks if a database exists.
    /// </summary>
    public class IfDatabaseExistsTask : IfExistsTask, ITask
    {
        /* ITask Interface */
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
        /* Some constructors */
        public IfDatabaseExistsTask()
        {
        }

        public IfDatabaseExistsTask(string databaseName) : this()
        {
            ObjectName = databaseName;
        }


        /* Static methods for convenience */
        public static bool IsExisting(string databaseName) => new IfDatabaseExistsTask(databaseName).Exists();
        public static bool IsExisting(IConnectionManager connectionManager, string databaseName)
            => new IfDatabaseExistsTask(databaseName) { ConnectionManager = connectionManager }.Exists();
    }
}