using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Drops a table if the table exists.
    /// </summary>
    public class IfTableOrViewExistsTask : IfExistsTask, ITask
    {
        /* ITask Interface */
        internal override string GetSql()
        {
            if (this.ConnectionType == ConnectionManagerType.SQLite)
            {
                return $@"SELECT 1 FROM sqlite_master WHERE name='{ObjectName}';";
            }
            else if (this.ConnectionType == ConnectionManagerType.SqlServer)
            {
                return  $@"IF ( OBJECT_ID('{ObjectName}', 'U') IS NOT NULL OR OBJECT_ID('{ObjectName}', 'V') IS NOT NULL)
    SELECT 1";
            }
            else if (this.ConnectionType == ConnectionManagerType.MySql)
            {
                return $@"SELECT EXISTS(
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
    AND ( table_name = '{ObjectName}' OR CONCAT(table_catalog, '.', table_name) = '{ObjectName}')
) AS 'DoesExist'";
            }
            else if (this.ConnectionType == ConnectionManagerType.Postgres)
            {
                return $@"SELECT EXISTS(
    SELECT table_name
    FROM information_schema.tables
    WHERE table_catalog = CURRENT_DATABASE()
    AND ( table_name = '{ObjectName}' OR CONCAT(table_schema, '.', table_name) = '{ObjectName}')
)";
            }
            else
            {
                return string.Empty;
            }
        }

        /* Some constructors */
        public IfTableOrViewExistsTask()
        {
        }

        public IfTableOrViewExistsTask(string tableName) : this()
        {
            ObjectName = tableName;
        }


        /* Static methods for convenience */
        public static bool IsExisting(string objectName) => new IfTableOrViewExistsTask(objectName).Exists();
        public static bool IsExisting(IConnectionManager connectionManager, string objectName)
            => new IfTableOrViewExistsTask(objectName) { ConnectionManager = connectionManager }.Exists();


        public static void ThrowExceptionIfNotExists(IConnectionManager connectionManager, string tableName)
        {
            bool tableExists = new IfTableOrViewExistsTask(tableName)
            {
                ConnectionManager = connectionManager,
                DisableLogging = true
            }.Exists();
            if (!tableExists)
                throw new ETLBoxException($"An table {tableName} does not exists in the database!");
        }
    }
}