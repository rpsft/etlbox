using ETLBox.Connection;
using ETLBox.Exceptions;
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
    public class GetDatabaseListTask : ControlFlowTask
    {
        /// <inheritdoc/>
        public override string TaskName => $"Get names of all databases";

        /// <summary>
        /// Queries the server for all user database names. The result is stored in <see cref="DatabaseNames"/>
        /// </summary>
        public void Execute()
        {
            if (!DbConnectionManager.SupportDatabases)
                throw new ETLBoxNotSupportedException("This task is not supported!");

            DatabaseNames = new List<string>();
            new SqlTask(this, Sql)
            {
                Actions = new List<Action<object>>() {
                    name => DatabaseNames.Add((string)name)
                }
            }.ExecuteReader();

            if (ConnectionType == ConnectionManagerType.MySql)
                DatabaseNames.RemoveAll(m => new List<string>()
                { "information_schema", "mysql", "performance_schema","sys"}.Contains(m));
        }


        /// <summary>
        /// A list containing all databases after executing.
        /// </summary>
        public List<string> DatabaseNames { get; set; }

        /// <summary>
        /// The sql code generated to query all database names
        /// </summary>
        public string Sql
        {
            get
            {
                if (ConnectionType == ConnectionManagerType.SqlServer)
                {
                    return $"SELECT [name] FROM master.dbo.sysdatabases WHERE dbid > 4";
                }
                else if (ConnectionType == ConnectionManagerType.MySql)
                {
                    return $"SHOW DATABASES";
                }
                else if (ConnectionType == ConnectionManagerType.Postgres)
                {
                    return "SELECT datname FROM pg_database WHERE datistemplate=false";
                }
                else
                {
                    throw new ETLBoxNotSupportedException("This database is not supported!");
                }
            }
        }

        public GetDatabaseListTask()
        {

        }

        public GetDatabaseListTask GetList()
        {
            Execute();
            return this;
        }

        /// <summary>
        /// Runs sql code to determine all user database names.
        /// </summary>
        /// <returns>A list of all user database names</returns>
        public static List<string> List()
            => new GetDatabaseListTask().GetList().DatabaseNames;

        /// <summary>
        /// Runs sql code to determine all user database names.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the server you want to connect</param>
        /// <returns>A list of all user database names</returns>
        public static List<string> List(IConnectionManager connectionManager)
            => new GetDatabaseListTask() { ConnectionManager = connectionManager }.GetList().DatabaseNames;

    }
}
