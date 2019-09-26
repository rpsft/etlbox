using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Drops a table if the table exists.
    /// </summary>
    public class IfIndexExistsTask : IfExistsTask, ITask
    {
        public string TableName { get; set; }
        /* ITask Interface */
        internal override string GetSql()
        {
            if (this.ConnectionType == ConnectionManagerType.SQLite)
            {
                return $@"
SELECT 1 FROM sqlite_master WHERE name='{ObjectName}' AND type='index';
";
            }
            else if (this.ConnectionType == ConnectionManagerType.SqlServer)
            {
                return
    $@"
IF EXISTS (SELECT *  FROM sys.indexes  WHERE name='{ObjectName}')
    SELECT 1
";
            }
            else if (this.ConnectionType == ConnectionManagerType.MySql)
            {
                return $@"
SELECT 1
FROM information_schema.statistics 
WHERE table_schema = DATABASE()
  AND ( table_name = '{TableName}' 
  OR CONCAT(table_name,'.',table_catalog) = '{TableName}')
  AND index_name = '{ObjectName}'
GROUP BY index_name
";
            }
            else if (this.ConnectionType == ConnectionManagerType.Postgres)
            {
                return $@"
SELECT     1
FROM       pg_indexes
WHERE     ( CONCAT(schemaname,'.',tablename) = '{TableName}'
            OR tablename = '{TableName}' )
            AND indexname = '{ObjectName}'
";
            }
            else
            {
                return string.Empty;
            }
        }

        /* Some constructors */
        public IfIndexExistsTask() : base()
        {
        }

        public IfIndexExistsTask(string indexName, string tableName) : this()
        {
            ObjectName = indexName;
            TableName = tableName;
        }


        /* Static methods for convenience */
        public static bool IsExisting(string indexName, string tableName) => new IfIndexExistsTask(indexName, tableName).Exists();
        public static bool IsExisting(IConnectionManager connectionManager, string indexName, string tableName)
            => new IfIndexExistsTask(indexName, tableName) { ConnectionManager = connectionManager }.Exists();

    }
}