using ETLBox.Connection;
using ETLBox.Exceptions;
using ETLBox.Helper;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Creates a schema if the schema doesn't exists. For MySql or MariaDb, use the CreateDatabaseTask instead.
    /// </summary>
    /// <example>
    /// <code>
    /// CreateSchemaTask.Create("demo");
    /// </code>
    /// </example>
    public class CreateSchemaTask : ControlFlowTask
    {
        /// <inheritdoc/>
        public override string TaskName => $"Create schema {SchemaName}";

        /// <summary>
        /// Runs the sql that creates the schema. Works only if the database does support schema (for MySql, use the CreateDatabaseTask instead)
        /// </summary>
        public void Execute()
        {
            if (!DbConnectionManager.SupportSchemas)
                throw new ETLBoxNotSupportedException("This task is not supported!");

            bool schemaExists = new IfSchemaExistsTask(SchemaName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (!schemaExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /// <summary>
        /// The name of the schema
        /// </summary>
        public string SchemaName { get; set; }

        /// <summary>
        /// The formatted schema name
        /// </summary>
        public ObjectNameDescriptor ON => new ObjectNameDescriptor(SchemaName, QB, QE);

        /// <summary>
        /// The sql that is used to create the schema.
        /// </summary>
        public string Sql => $@"CREATE SCHEMA {ON.QuotatedObjectName}";

        public CreateSchemaTask()
        {

        }
        public CreateSchemaTask(string schemaName) : this()
        {
            this.SchemaName = schemaName;
        }

        /// <summary>
        /// Creates a schema if the schema doesn't exists. For MySql, use the CreateDatabaseTask instead.
        /// </summary>
        /// <param name="schemaName">The name of the schema</param>
        public static void Create(string schemaName) => new CreateSchemaTask(schemaName).Execute();

        /// <summary>
        /// Creates a schema if the schema doesn't exists. For MySql, use the CreateDatabaseTask instead
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="schemaName">The name of the schema</param>
        public static void Create(IConnectionManager connectionManager, string schemaName)
            => new CreateSchemaTask(schemaName) { ConnectionManager = connectionManager }.Execute();


    }
}
