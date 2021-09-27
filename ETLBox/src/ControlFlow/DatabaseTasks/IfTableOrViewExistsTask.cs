using ETLBox.Connection;
using ETLBox.Exceptions;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Checks if a table exists.
    /// </summary>
    public sealed class IfTableOrViewExistsTask : IfExistsTask
    {
        internal override string GetSql() {
            if (this.ConnectionType == ConnectionManagerType.SQLite) {
                return $@"SELECT 1 FROM sqlite_master WHERE name='{ON.UnquotatedObjectName}';";
            } else if (this.ConnectionType == ConnectionManagerType.SqlServer) {
                return $@"IF ( OBJECT_ID('{ON.QuotatedFullName}', 'U') IS NOT NULL OR OBJECT_ID('{ON.QuotatedFullName}', 'V') IS NOT NULL)
    SELECT 1";
            } else if (this.ConnectionType == ConnectionManagerType.MySql) {
                return $@"SELECT EXISTS(
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = DATABASE()
    AND ( table_name = '{ON.UnquotatedFullName}' OR CONCAT(table_catalog, '.', table_name) = '{ON.UnquotatedFullName}')
) AS 'DoesExist'";
            } else if (this.ConnectionType == ConnectionManagerType.Postgres) {
                return $@"
SELECT EXISTS(
               SELECT pns.nspname, pc.relname
               FROM pg_catalog.pg_class pc,
                    pg_catalog.pg_namespace pns
               WHERE pns.oid = pc.relnamespace
                --AND pns.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                 AND pc.relkind IN ('r', 'v') --r = relations/table, v = view, m = materialized view
                 AND (pc.relname = '{ON.UnquotatedFullName}' OR CONCAT(pns.nspname, '.', pc.relname) = '{ON.UnquotatedFullName}')
           )
";
            } else if (this.ConnectionType == ConnectionManagerType.Oracle) {
                return $@"
 SELECT 
CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS ""Count""
FROM all_objects
WHERE object_type IN('TABLE', 'VIEW')
AND(object_name = '{ON.UnquotatedFullName}'
    OR owner || '.' || object_name = '{ON.UnquotatedFullName}'
    )
";
            } else if (this.ConnectionType == ConnectionManagerType.Db2) {
                //                return $@"SELECT 1 FROM SYSIBM.SYSTABLES
                //WHERE NAME = '{ON.UnquotatedFullName}'
                //OR ( TRIM(CREATOR) || '.' || NAME = '{ON.UnquotatedFullName}' )
                //";
                //                return $@"SELECT 1 FROM syscat.tables 
                //WHERE tabname = '{ON.UnquotatedFullName}'
                //OR ( TRIM(tabschema) || '.' || tabname = '{ON.UnquotatedFullName}' )
                //";
                return $@"
SELECT 1 FROM SYSIBM.SQLTABLES
WHERE (TABLE_NAME = '{ON.UnquotatedFullName}'
    OR (TRIM(TABLE_SCHEM) || '.' || TABLE_NAME = '{ON.UnquotatedFullName}')
    )
AND TABLE_TYPE IN ('VIEW','TABLE')
";
            } else if (this.ConnectionType == ConnectionManagerType.Access) {
                var connMan = this.DbConnectionManager.CloneIfAllowed();// as AccessOdbcConnectionManager;
                var connDbObject = connMan as IConnectionManagerDbObjects;
                DoesExist = connDbObject?.CheckIfTableOrViewExists(ON.UnquotatedFullName) ?? false;
                connMan.CloseIfAllowed();
                return string.Empty;
                //return $@"SELECT * FROM MSysObjects WHERE Type=1 AND Flags=0  AND Name = '{ON.UnquotatedFullName}'";
            } else {
                return string.Empty;
            }
        }

        public IfTableOrViewExistsTask() {
        }

        public IfTableOrViewExistsTask(string tableName) : this() {
            ObjectName = tableName;
        }

        public IfTableOrViewExistsTask(IConnectionManager connectionManager, string tableName) : this(tableName) {
            this.ConnectionManager = connectionManager;
        }

        /// <summary>
        /// Ćhecks if the table or view exists
        /// </summary>
        /// <param name="objectName">The table or view name that you want to check for existence</param>
        /// <returns>True if the table or view exists</returns>
        public static bool IsExisting(string objectName) => new IfTableOrViewExistsTask(objectName).Exists();

        /// <summary>
        /// Ćhecks if the table or view exists
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="objectName">The table or view name that you want to check for existence</param>
        /// <returns>True if the table or view exists</returns>
        public static bool IsExisting(IConnectionManager connectionManager, string objectName)
            => new IfTableOrViewExistsTask(objectName) { ConnectionManager = connectionManager }.Exists();

    }
}