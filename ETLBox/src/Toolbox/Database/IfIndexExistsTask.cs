using ETLBox.ConnectionManager;

namespace ETLBox.ControlFlow
{
    /// <summary>
    /// Checks if an index exists.
    /// </summary>
    public class IfIndexExistsTask : IfExistsTask, ITask
    {
        /* ITask Interface */
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
            else
            {
                return string.Empty;
            }
        }

        /* Some constructors */
        public IfIndexExistsTask()
        {
        }

        public IfIndexExistsTask(string indexName, string tableName) : this()
        {
            ObjectName = indexName;
            OnObjectName = tableName;
        }


        /* Static methods for convenience */
        public static bool IsExisting(string indexName, string tableName) => new IfIndexExistsTask(indexName, tableName).Exists();
        public static bool IsExisting(IConnectionManager connectionManager, string indexName, string tableName)
            => new IfIndexExistsTask(indexName, tableName) { ConnectionManager = connectionManager }.Exists();

    }
}