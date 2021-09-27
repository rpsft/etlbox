using ETLBox.Connection;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Checks if an index exists.
    /// </summary>
    public sealed class IfIndexExistsTask : IfExistsTask, ILoggableTask
    {
        internal override string GetSql() {
            if (this.ConnectionType == ConnectionManagerType.SQLite) {
                return $@"
SELECT 1 FROM sqlite_master WHERE name='{ON.UnquotatedObjectName}' AND type='index';
";
            } else if (this.ConnectionType == ConnectionManagerType.SqlServer) {
                return
    $@"
IF EXISTS (SELECT *  FROM sys.indexes  WHERE name='{ON.UnquotatedObjectName}' AND object_id = OBJECT_ID('{OON.QuotatedFullName}'))
    SELECT 1
";
            } else if (this.ConnectionType == ConnectionManagerType.MySql) {
                return $@"
SELECT 1
FROM information_schema.statistics 
WHERE table_schema = DATABASE()
  AND ( table_name = '{OON.UnquotatedFullName}' 
  OR CONCAT(table_name,'.',table_catalog) = '{OON.UnquotatedFullName}')
  AND index_name = '{ON.UnquotatedObjectName}'
GROUP BY index_name
";
            } else if (this.ConnectionType == ConnectionManagerType.Postgres) {
                return $@"
SELECT     1
FROM       pg_indexes
WHERE     ( CONCAT(schemaname,'.',tablename) = '{OON.UnquotatedFullName}'
            OR tablename = '{OON.UnquotatedFullName}' )
            AND indexname = '{ON.UnquotatedObjectName}'
";
            } else if (this.ConnectionType == ConnectionManagerType.Oracle) {
                return $@"
SELECT 1 
FROM ALL_INDEXES aidx
WHERE ( aidx.TABLE_NAME  = '{OON.UnquotatedFullName}'
        OR aidx.TABLE_OWNER || '.' || aidx.TABLE_NAME = '{OON.UnquotatedFullName}'
       )
AND aidx.INDEX_NAME   = '{ON.UnquotatedObjectName}'
";
            } else if (this.ConnectionType == ConnectionManagerType.Db2) {
                //                return $@"
                //SELECT 1
                //FROM SYSIBM.SYSINDEXES i
                //INNER JOIN SYSIBM.SYSTABLES t on 
                //    t.creator = i.tbcreator and t.name= i.tbname
                //WHERE t.type IN ('T')
                //AND ( t.name = '{OON.UnquotatedFullName}'
                //      OR ( TRIM(t.creator) || '.' || TRIM(t.name) = '{OON.UnquotatedFullName}' )
                //    )
                //AND ( i.name = '{ON.UnquotatedFullName}'
                //      OR ( TRIM(i.creator) || '.' || i.name= '{ON.UnquotatedFullName}' )
                //    )
                //";
                //                return $@"
                //SELECT 1
                //FROM syscat.indexes i
                //INNER JOIN syscat.tables t on 
                //    t.tabschema = i.tabschema and t.tabname = i.tabname
                //WHERE t.type IN ('T')
                //AND ( t.tabname = '{OON.UnquotatedFullName}'
                //      OR ( TRIM(t.tabschema) || '.' || TRIM(t.tabname) = '{OON.UnquotatedFullName}' )
                //    )
                //AND ( i.indname = '{ON.UnquotatedFullName}'
                //      OR ( TRIM(i.indschema) || '.' || i.indname = '{ON.UnquotatedFullName}' )
                //    )
                //";
                return $@"
SELECT 1
FROM SYSIBM.SQLSTATISTICS i
INNER JOIN SYSIBM.SQLTABLES t on
    t.TABLE_SCHEM = i.TABLE_SCHEM and t.TABLE_NAME = i.TABLE_NAME
WHERE t.TABLE_TYPE IN ('TABLE')
AND ( t.TABLE_NAME = '{OON.UnquotatedFullName}'
      OR ( TRIM(t.TABLE_SCHEM) || '.' || TRIM(t.TABLE_NAME) = '{OON.UnquotatedFullName}' )
    )
AND ( i.INDEX_NAME = '{ON.UnquotatedFullName}'
      OR ( TRIM(i.INDEX_QUALIFIER) || '.' || i.INDEX_NAME = '{ON.UnquotatedFullName}' )
    );
";
            } else {
                return string.Empty;
            }
        }

        public IfIndexExistsTask() {
        }

        public IfIndexExistsTask(string indexName, string tableName) : this() {
            ObjectName = indexName;
            OnObjectName = tableName;
        }

        /// <summary>
        /// Ćhecks if the index exists
        /// </summary>
        /// <param name="indexName">The index name that you want to check for existence</param>
        /// <param name="tableName">The table name on which the index is based on</param>
        /// <returns>True if the index exists</returns>
        public static bool IsExisting(string indexName, string tableName) => new IfIndexExistsTask(indexName, tableName).Exists();

        /// <summary>
        /// Ćhecks if the index exists
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="indexName">The index name that you want to check for existence</param>
        /// <param name="tableName">The table name on which the index is based on</param>
        /// <returns>True if the index exists</returns>
        public static bool IsExisting(IConnectionManager connectionManager, string indexName, string tableName)
            => new IfIndexExistsTask(indexName, tableName) { ConnectionManager = connectionManager }.Exists();

    }
}