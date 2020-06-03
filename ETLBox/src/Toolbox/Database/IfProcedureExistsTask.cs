using ETLBox.Connection;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Checks if a procedure exists.
    /// </summary>
    public class IfProcedureExistsTask : IfExistsTask, ITask
    {
        internal override string GetSql()
        {
            if (!DbConnectionManager.SupportProcedures)
                throw new ETLBoxNotSupportedException("This task is not supported!");

            if (this.ConnectionType == ConnectionManagerType.SqlServer)
            {
                return
    $@"
IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND object_id = object_id('{ON.QuotatedFullName}'))
    SELECT 1
";
            }
            else if (this.ConnectionType == ConnectionManagerType.MySql)
            {
                return $@"
 SELECT 1 
FROM information_schema.routines 
WHERE routine_schema = DATABASE()
   AND ( routine_name = '{ON.UnquotatedFullName}' OR
        CONCAT(routine_catalog, '.', routine_name) = '{ON.UnquotatedFullName}' )  
";
            }
            else if (this.ConnectionType == ConnectionManagerType.Postgres)
            {
                return $@"
SELECT 1
FROM pg_catalog.pg_proc
JOIN pg_namespace 
  ON pg_catalog.pg_proc.pronamespace = pg_namespace.oid
WHERE ( CONCAT(pg_namespace.nspname,'.',proname) = '{ON.UnquotatedFullName}'
            OR proname = '{ON.UnquotatedFullName}' )
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