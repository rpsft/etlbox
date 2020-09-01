using ETLBox.Connection;
using ETLBox.Exceptions;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Checks if a procedure exists.
    /// </summary>
    public class IfProcedureExistsTask : IfExistsTask
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
            else if (this.ConnectionType == ConnectionManagerType.Oracle)
            {
                return $@"
SELECT 1
FROM ALL_OBJECTS
WHERE object_type = 'PROCEDURE'
AND ( object_name = '{ON.UnquotatedFullName}'
 OR  owner || '.' || object_name = '{ON.UnquotatedFullName}'
    )
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


        /// <summary>
        /// Ćhecks if the procedure exists
        /// </summary>
        /// <param name="procedureName">The procedure name that you want to check for existence</param>
        /// <returns>True if the procedure exists</returns>
        public static bool IsExisting(string procedureName) => new IfProcedureExistsTask(procedureName).Exists();

        /// <summary>
        /// Ćhecks if the procedure exists
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="procedureName">The procedure name that you want to check for existence</param>
        /// <returns>True if the procedure exists</returns>
        public static bool IsExisting(IConnectionManager connectionManager, string procedureName)
            => new IfProcedureExistsTask(procedureName) { ConnectionManager = connectionManager }.Exists();

    }
}