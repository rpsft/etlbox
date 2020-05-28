using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Checks if a schema exists. In MySql, use the IfDatabaseExistsTask instead.
    /// </summary>
    public class IfSchemaExistsTask : IfExistsTask, ITask
    {
        /* ITask Interface */
        internal override string GetSql()
        {
            if (!DbConnectionManager.SupportSchemas)
                throw new ETLBoxNotSupportedException("This task is not supported!");

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
                return $@"SELECT 1 FROM information_schema.schemata WHERE schema_name = '{ON.UnquotatedObjectName}';
";
            }
            else
            {
                return string.Empty;
            }
        }

        /* Some constructors */
        public IfSchemaExistsTask()
        {
        }

        public IfSchemaExistsTask(string schemaName) : this()
        {
            ObjectName = schemaName;
        }


        /* Static methods for convenience */
        public static bool IsExisting(string schemaName)
            => new IfSchemaExistsTask(schemaName).Exists();
        public static bool IsExisting(IConnectionManager connectionManager, string schemaName)
            => new IfSchemaExistsTask(schemaName) { ConnectionManager = connectionManager }.Exists();

    }
}