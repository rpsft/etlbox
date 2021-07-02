using ETLBox.Connection;
using System;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Checks if a schema exists. In MySql or MariaDb, use the IfDatabaseExistsTask instead.
    /// </summary>
    public sealed class IfSchemaExistsTask : IfExistsTask
    {
        internal override string GetSql()
        {
            if (!DbConnectionManager.SupportSchemas)
                throw new NotSupportedException($"This task is not supported with the current connection manager ({ConnectionType})");

            if (this.ConnectionType == ConnectionManagerType.SqlServer)
            {
                return
    $@"
IF EXISTS (SELECT schema_name(schema_id) FROM sys.schemas WHERE schema_name(schema_id) = '{ON.UnquotatedObjectName}')
    SELECT 1
";
            }
            else if (this.ConnectionType == ConnectionManagerType.Postgres)
            {
                return $@"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{ON.UnquotatedObjectName}';";
            }
            else if (this.ConnectionType == ConnectionManagerType.Db2)
            {
                //return $@"SELECT 1 FROM SYSIBM.SYSSCHEMATA WHERE NAME = '{ON.UnquotatedObjectName}'";
                return $@"SELECT 1 FROM syscat.SCHEMATA WHERE SCHEMANAME = '{ON.UnquotatedObjectName}'";
            }
            else
            {
                return string.Empty;
            }
        }

        public IfSchemaExistsTask()
        {
        }

        public IfSchemaExistsTask(string schemaName) : this()
        {
            ObjectName = schemaName;
        }


        /// <summary>
        /// Ćhecks if the schema exists
        /// </summary>
        /// <param name="schemaName">The schema name that you want to check for existence</param>
        /// <returns>True if the schema exists</returns>
        public static bool IsExisting(string schemaName)
            => new IfSchemaExistsTask(schemaName).Exists();

        /// <summary>
        /// Ćhecks if the schema exists
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="schemaName">The schema name that you want to check for existence</param>
        /// <returns>True if the schema exists</returns>
        public static bool IsExisting(IConnectionManager connectionManager, string schemaName)
            => new IfSchemaExistsTask(schemaName) { ConnectionManager = connectionManager }.Exists();

    }
}