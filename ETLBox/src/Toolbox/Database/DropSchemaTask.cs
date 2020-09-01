using ETLBox.Connection;
using ETLBox.Exceptions;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Drops a schema. Use DropIfExists to drop a schema only if it exists. For MySql or MariaDb, use the DropDatabase task instead.
    /// </summary>
    public class DropSchemaTask : DropTask<IfSchemaExistsTask>, ILoggableTask
    {
        internal override string GetSql()
        {
            if (!DbConnectionManager.SupportSchemas)
                throw new ETLBoxNotSupportedException("This task is not supported!");

            string sql = $@"DROP SCHEMA {ON.QuotatedFullName}";
            return sql;
        }

        public DropSchemaTask()
        {
        }

        public DropSchemaTask(string schemaName) : this()
        {
            ObjectName = schemaName;
        }

        /// <summary>
        /// Drops a schema. For MySql, use the DropDatabase task instead.
        /// </summary>
        /// <param name="schemaName">Name of the schema to drop</param>
        public static void Drop(string schemaName)
            => new DropSchemaTask(schemaName).Drop();

        /// <summary>
        /// Drops a schema. For MySql, use the DropDatabase task instead.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="schemaName">Name of the schema to drop</param>
        public static void Drop(IConnectionManager connectionManager, string schemaName)
            => new DropSchemaTask(schemaName) { ConnectionManager = connectionManager }.Drop();

        /// <summary>
        /// Drops a schema if the schema exists. For MySql, use the DropDatabase task instead.
        /// </summary>
        /// <param name="schemaName">Name of the schema to drop</param>
        public static void DropIfExists(string schemaName)
            => new DropSchemaTask(schemaName).DropIfExists();

        /// <summary>
        /// Drops a schema if the schema exists. For MySql, use the DropDatabase task instead.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="schemaName">Name of the schema to drop</param>
        public static void DropIfExists(IConnectionManager connectionManager, string schemaName)
            => new DropSchemaTask(schemaName) { ConnectionManager = connectionManager }.DropIfExists();
    }


}
