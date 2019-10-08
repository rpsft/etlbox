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
        public override string TaskName => $"Create schema {SchemaName}";
        public void Execute()
        {
            if (ConnectionType == ConnectionManagerType.SQLite)
                throw new ETLBoxNotSupportedException("This task is not supported with SQLite!");
            if (ConnectionType == ConnectionManagerType.MySql)
                throw new ETLBoxNotSupportedException("This task is not supported with MySql! To create a database, use the CreateDatabaseTask instead.");
            bool schemaExists = new IfSchemaExistsTask(SchemaName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (!schemaExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string SchemaName { get; set; }
        public string Sql => $@"CREATE SCHEMA {QB}{SchemaName}{QE}";

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
