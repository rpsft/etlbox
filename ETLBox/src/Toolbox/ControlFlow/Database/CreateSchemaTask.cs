using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Creates a schema if the schema doesn't exists. For MySql, use the CreateDatabaseTask instead.
    /// </summary>
    /// <example>
    /// <code>
    /// CreateSchemaTask.Create("demo");
    /// </code>
    /// </example>
    [PublicAPI]
    public class CreateSchemaTask : GenericTask
    {
        /* ITask Interface */
        public override string TaskName => $"Create schema {SchemaName}";

        public void Execute()
        {
            if (!DbConnectionManager.SupportSchemas)
                throw new ETLBoxNotSupportedException("This task is not supported!");

            bool schemaExists = new IfSchemaExistsTask(SchemaName)
            {
                ConnectionManager = ConnectionManager,
                DisableLogging = true
            }.Exists();
            if (!schemaExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string SchemaName { get; set; }
        public ObjectNameDescriptor ON => new(SchemaName, QB, QE);
        public string Sql => $@"CREATE SCHEMA {ON.QuotatedObjectName}";

        public CreateSchemaTask() { }

        public CreateSchemaTask(string schemaName)
            : this()
        {
            SchemaName = schemaName;
        }

        public static void Create(string schemaName) => new CreateSchemaTask(schemaName).Execute();

        public static void Create(IConnectionManager connectionManager, string schemaName) =>
            new CreateSchemaTask(schemaName) { ConnectionManager = connectionManager }.Execute();
    }
}
