using ETLBox.Connection;
using ETLBox.Exceptions;
using ETLBox.Helper;
using System;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Creates a schema. For MySql or MariaDb, use the CreateDatabaseTask instead.
    /// The Create method will throw an exception if the schema already exists. 
    /// CreateIfNotExists will only create a schema if it doesn't exists.
    /// </summary>
    /// <example>
    /// <code>
    /// CreateSchemaTask.Create("demo");
    /// CreateSchemaTask.CreateIfNotExists("demo2");
    /// </code>
    /// </example>
    public sealed class CreateSchemaTask : ControlFlowTask
    {
        /// <inheritdoc/>
        public override string TaskName { get; set; } = $"Create schema";

        /// <summary>
        /// The name of the schema
        /// </summary>
        public string SchemaName { get; set; }

        /// <summary>
        /// Runs the sql that creates the schema. If the schema already exists, an Exception is thrown.
        /// Works only if the database does support schema (for MySql, use the CreateDatabaseTask instead)
        /// </summary>
        public void Create()
        {
            ThrowOnError = true;
            Execute();
        }

        /// <summary>
        /// Runs the sql that creates the schema. Schema is only created if the schema doesn't exists.
        /// Works only if the database does support schema (for MySql, use the CreateDatabaseTask instead)
        /// </summary>
        public void CreateIfNotExists() => Execute();

        /// <summary>
        /// The formatted schema name
        /// </summary>
        public ObjectNameDescriptor ON => new ObjectNameDescriptor(SchemaName, QB, QE);

        /// <summary>
        /// The sql that is used to create the schema.
        /// </summary>
        public string Sql => $@"CREATE SCHEMA {ON.QuotatedObjectName}{AuthorizationUserSql}";

        public string AuthorizationUser { get; set; }

        public CreateSchemaTask()
        {

        }
        public CreateSchemaTask(string schemaName) : this()
        {
            this.SchemaName = schemaName;
        }

        public CreateSchemaTask(string schemaName, string authorizationUser) : this(schemaName)
        {
            this.AuthorizationUser = authorizationUser;
        }

        string AuthorizationUserSql => !string.IsNullOrWhiteSpace(AuthorizationUser) ?
            $@" AUTHORIZATION {new ObjectNameDescriptor(AuthorizationUser, QB, QE).QuotatedFullName}" :
            string.Empty;
        internal bool ThrowOnError { get; set; }

        internal void Execute()
        {
            if (!DbConnectionManager.SupportSchemas)
                throw new NotSupportedException($"This task is not supported with the current connection manager ({ConnectionType})");

            bool schemaExists = new IfSchemaExistsTask(SchemaName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (schemaExists && ThrowOnError) throw new ETLBoxException($"Schema {SchemaName} already exists - can't create the schema!");
            if (!schemaExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /// <summary>
        /// Creates a schema. Throws an exception if the schema already exists. For MySql, use the CreateDatabaseTask instead.
        /// </summary>
        /// <param name="schemaName">The name of the schema</param>
        public static void Create(string schemaName) => new CreateSchemaTask(schemaName) { ThrowOnError = true }.Execute();

        /// <summary>
        /// Creates a schema. Throws an exception if the schema already exists. For MySql, use the CreateDatabaseTask instead.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="schemaName">The name of the schema</param>
        public static void Create(IConnectionManager connectionManager, string schemaName)
            => new CreateSchemaTask(schemaName) { ConnectionManager = connectionManager, ThrowOnError = true }.Execute();

        /// <summary>
        /// Creates a schema if the schema doesn't exists. For MySql, use the CreateDatabaseTask instead.
        /// </summary>
        /// <param name="schemaName">The name of the schema</param>
        public static void CreateIfNotExists(string schemaName)
            => new CreateSchemaTask(schemaName).Execute();

        /// <summary>
        /// Creates a schema if the schema doesn't exists. For MySql, use the CreateDatabaseTask instead
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="schemaName">The name of the schema</param>
        public static void CreateIfNotExists(IConnectionManager connectionManager, string schemaName)
            => new CreateSchemaTask(schemaName) { ConnectionManager = connectionManager }.Execute();

        /// <summary>
        /// Creates a schema. Throws an exception if the schema already exists. For MySql, use the CreateDatabaseTask instead.
        /// </summary>
        /// <param name="schemaName">The name of the schema</param>
        /// <param name="authorizationUser">Database user which is authorized for the schema</param>
        public static void Create(string schemaName, string authorizationUser)
            => new CreateSchemaTask(schemaName, authorizationUser) { ThrowOnError = true }.Execute();

        /// <summary>
        /// Creates a schema. Throws an exception if the schema already exists. For MySql, use the CreateDatabaseTask instead.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="schemaName">The name of the schema</param>
        /// <param name="authorizationUser">Database user which is authorized for the schema</param>
        public static void Create(IConnectionManager connectionManager, string schemaName, string authorizationUser)
            => new CreateSchemaTask(schemaName, authorizationUser) { ConnectionManager = connectionManager, ThrowOnError = true }.Execute();

        /// <summary>
        /// Creates a schema if the schema doesn't exists. For MySql, use the CreateDatabaseTask instead.
        /// </summary>
        /// <param name="schemaName">The name of the schema</param>
        /// <param name="authorizationUser">Database user which is authorized for the schema</param>
        public static void CreateIfNotExists(string schemaName, string authorizationUser)
            => new CreateSchemaTask(schemaName, authorizationUser).Execute();

        /// <summary>
        /// Creates a schema if the schema doesn't exists. For MySql, use the CreateDatabaseTask instead
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="schemaName">The name of the schema</param>
        /// <param name="authorizationUser">Database user which is authorized for the schema</param>
        public static void CreateIfNotExists(IConnectionManager connectionManager, string schemaName, string authorizationUser)
            => new CreateSchemaTask(schemaName, authorizationUser) { ConnectionManager = connectionManager }.Execute();

    }
}
