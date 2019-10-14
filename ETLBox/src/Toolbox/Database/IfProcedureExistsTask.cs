using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Checks if a procedure exists.
    /// </summary>
    public class IfProcedureExistsTask : IfExistsTask, ITask
    {
        internal override string GetSql()
        {
            if (this.ConnectionType == ConnectionManagerType.SQLite)
            {
                throw new ETLBoxNotSupportedException("This task is not supported with SQLite!");
            }
            else if (this.ConnectionType == ConnectionManagerType.SqlServer)
            {
                return
    $@"
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND object_id = object_id('{ObjectName}'))
    SELECT 1
";
            }
            else if (this.ConnectionType == ConnectionManagerType.MySql)
            {
                return $@"
 SELECT 1 
FROM information_schema.routines 
WHERE routine_schema = DATABASE()
   AND ( routine_name = '{ObjectName}' OR
        CONCAT(routine_catalog, '.', routine_name) = '{ObjectName}' )  
";
            }
            else if (this.ConnectionType == ConnectionManagerType.Postgres)
            {
                return $@"
SELECT 1
FROM pg_catalog.pg_proc
JOIN pg_namespace 
  ON pg_catalog.pg_proc.pronamespace = pg_namespace.oid
WHERE ( CONCAT(pg_namespace.nspname,'.',proname) = '{ObjectName}'
            OR proname = '{ObjectName}' )
";
            }
            else
            {
                return string.Empty;
            }
        }

        public IfProcedureExistsTask()
        {
        }

        public IfProcedureExistsTask(string procedureName) : this()
        {
            ObjectName = procedureName;
        }


        public static bool IsExisting(string procedureName) => new IfProcedureExistsTask(procedureName).Exists();
        public static bool IsExisting(IConnectionManager connectionManager, string procedureName)
            => new IfProcedureExistsTask(procedureName) { ConnectionManager = connectionManager }.Exists();

    }
}