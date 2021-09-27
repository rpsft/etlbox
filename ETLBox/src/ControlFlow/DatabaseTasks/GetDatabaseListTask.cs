using ETLBox.Connection;
using ETLBox.Helper;
using System;
using System.Collections.Generic;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Returns a list of all user databases on the server. Make sure to connect with the correct permissions!
    /// In MySql, this will return a list of all schemas.
    /// </summary>
    /// <example>
    /// <code>
    /// GetDatabaseListTask.List();
    /// </code>
    /// </example>
    public sealed class GetDatabaseListTask : GetListTask
    {
        /// <inheritdoc/>
        public override string TaskName { get; set; } = $"Get names of all databases";

        public GetDatabaseListTask() {

        }

        internal override string GetSql() {
            if (!DbConnectionManager.SupportDatabases)
                throw new NotSupportedException($"The connection type {this.ConnectionType} does not support databases!");

            if (ConnectionType == ConnectionManagerType.SqlServer) {
                return $"SELECT [name] FROM master.dbo.sysdatabases WHERE dbid > 4";
            } else if (ConnectionType == ConnectionManagerType.MySql) {
                return $"SHOW DATABASES";
            } else if (ConnectionType == ConnectionManagerType.Postgres) {
                return "SELECT datname FROM pg_database WHERE datistemplate=false";
            } else {
                throw new NotSupportedException($"The database type {this.ConnectionType} is not supported!");
            }
        }

        internal override void CleanUpRetrievedList() {
            if (ConnectionType == ConnectionManagerType.MySql)
                ObjectNames.RemoveAll(m => new List<string>()
                { "information_schema", "mysql", "performance_schema","sys"}.Contains(m.UnquotatedObjectName));
        }

        /// <summary>
        /// Runs sql code to determine all user database names.
        /// </summary>
        /// <returns>A list of all user database names</returns>
        public static List<ObjectNameDescriptor> ListAll()
            => new GetDatabaseListTask().RetrieveAll().ObjectNames;

        /// <summary>
        /// Runs sql code to determine all user database names.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the server you want to connect</param>
        /// <returns>A list of all user database names</returns>
        public static List<ObjectNameDescriptor> ListAll(IConnectionManager connectionManager)
            => new GetDatabaseListTask() { ConnectionManager = connectionManager }.RetrieveAll().ObjectNames;

    }
}
