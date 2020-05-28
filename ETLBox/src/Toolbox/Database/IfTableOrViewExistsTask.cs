using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Checks if a table exists.
    /// </summary>
    public class IfTableOrViewExistsTask : IfExistsTask, ITask
    {
        internal override string GetSql()
        {
            if (this.ConnectionType == ConnectionManagerType.SQLite)
            {
                return $@"SELECT 1 FROM sqlite_master WHERE name='{ON.UnquotatedObjectName}';";
            }
            else if (this.ConnectionType == ConnectionManagerType.SqlServer)
            {
                return $@"IF ( OBJECT_ID('{ON.QuotatedFullName}', 'U') IS NOT NULL OR OBJECT_ID('{ON.QuotatedFullName}', 'V') IS NOT NULL)
    SELECT 1";
            }
            else if (this.ConnectionType == ConnectionManagerType.MySql)
            {
                return $@"SELECT EXISTS(
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
    AND ( table_name = '{ON.UnquotatedFullName}' OR CONCAT(table_catalog, '.', table_name) = '{ON.UnquotatedFullName}')
) AS 'DoesExist'";
            }
            else if (this.ConnectionType == ConnectionManagerType.Postgres)
            {
                return $@"SELECT EXISTS(
    SELECT table_name
    FROM information_schema.tables
    WHERE table_catalog = CURRENT_DATABASE()
    AND ( table_name = '{ON.UnquotatedFullName}' OR CONCAT(table_schema, '.', table_name) = '{ON.UnquotatedFullName}')
)";
            }
            else if (this.ConnectionType == ConnectionManagerType.Access)
            {
                var connMan = this.DbConnectionManager.CloneIfAllowed();// as AccessOdbcConnectionManager;
                DoesExist = connMan?.CheckIfTableOrViewExists(ON.UnquotatedFullName) ?? false;
                connMan.CloseIfAllowed();
                return string.Empty;
                //return $@"SELECT * FROM MSysObjects WHERE Type=1 AND Flags=0  AND Name = '{ON.UnquotatedFullName}'";
            }
            else
            {
                return string.Empty;
            }
        }

        public IfTableOrViewExistsTask()
        {
        }

        public IfTableOrViewExistsTask(string tableName) : this()
        {
            ObjectName = tableName;
        }

        public IfTableOrViewExistsTask(IConnectionManager connectionManager, string tableName) : this(tableName)
        {
            this.ConnectionManager = connectionManager;
        }

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
                throw new ETLBoxException($"A table {tableName} does not exists in the database!");
        }
    }
}