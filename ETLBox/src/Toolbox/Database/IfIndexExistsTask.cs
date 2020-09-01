using ETLBox.Connection;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Checks if an index exists.
    /// </summary>
    public class IfIndexExistsTask : IfExistsTask, ILoggableTask
    {
        internal override string GetSql()
        {
            if (this.ConnectionType == ConnectionManagerType.SQLite)
            {
                return $@"
SELECT 1 FROM sqlite_master WHERE name='{ON.UnquotatedObjectName}' AND type='index';
";
            }
            else if (this.ConnectionType == ConnectionManagerType.SqlServer)
            {
                return
    $@"
IF EXISTS (SELECT *  FROM sys.indexes  WHERE name='{ON.UnquotatedObjectName}' AND object_id = OBJECT_ID('{OON.QuotatedFullName}'))
    SELECT 1
";
            }
            else if (this.ConnectionType == ConnectionManagerType.MySql)
            {
                return $@"
SELECT 1
FROM information_schema.statistics 
WHERE table_schema = DATABASE()
  AND ( table_name = '{OON.UnquotatedFullName}' 
  OR CONCAT(table_name,'.',table_catalog) = '{OON.UnquotatedFullName}')
  AND index_name = '{ON.UnquotatedObjectName}'
GROUP BY index_name
";
            }
            else if (this.ConnectionType == ConnectionManagerType.Postgres)
            {
                return $@"
SELECT     1
FROM       pg_indexes
WHERE     ( CONCAT(schemaname,'.',tablename) = '{OON.UnquotatedFullName}'
            OR tablename = '{OON.UnquotatedFullName}' )
            AND indexname = '{ON.UnquotatedObjectName}'
";
            }
            else if(this.ConnectionType == ConnectionManagerType.Oracle)
            {
                return $@"
SELECT 1 
FROM ALL_INDEXES aidx
WHERE ( aidx.TABLE_NAME  = '{OON.UnquotatedFullName}'
        OR aidx.TABLE_OWNER || '.' || aidx.TABLE_NAME = '{OON.UnquotatedFullName}'
       )
AND aidx.INDEX_NAME   = '{ON.UnquotatedObjectName}'
";
            }
            else
            {
                return string.Empty;
            }
        }

        public IfIndexExistsTask()
        {
        }

        public IfIndexExistsTask(string indexName, string tableName) : this()
        {
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