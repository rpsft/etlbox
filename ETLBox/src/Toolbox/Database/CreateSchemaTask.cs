using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Creates a schema if the schema doesn't exists.
    /// </summary>
    /// <example>
    /// <code>
    /// CreateSchemaTask.Create("demo");
    /// </code>
    /// </example>
    public class CreateSchemaTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CREATESCHEMA";
        public override string TaskName => $"Create schema {SchemaName}";
        public override void Execute()
        {
            if (ConnectionType == ConnectionManagerType.SQLLite)
                throw new ETLBoxNotSupportedException("This task is not supported with SQLite!");
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string SchemaName { get; set; }
        public string Sql => $@"
IF NOT EXISTS (SELECT schema_name(schema_id) FROM sys.schemas WHERE schema_name(schema_id) = '{SchemaName}')
BEGIN
	EXEC sp_executesql N'CREATE SCHEMA [{SchemaName}]'
END";

        public CreateSchemaTask()
        {

        }
        public CreateSchemaTask(string schemaName) : this()
        {
            this.SchemaName = schemaName;
        }

        public static void Create(string schemaName) => new CreateSchemaTask(schemaName).Execute();
        public static void Create(IConnectionManager connectionManager, string schemaName)
            => new CreateSchemaTask(schemaName) { ConnectionManager = connectionManager }.Execute();


    }
}
