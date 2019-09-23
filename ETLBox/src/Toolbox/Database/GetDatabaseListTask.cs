using ALE.ETLBox.ConnectionManager;
using System;
using System.Collections.Generic;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Returns a list of all databases on the server (make sure to connect with the correct permissions).
    /// </summary>
    /// <example>
    /// <code>
    /// GetDatabaseListTask.List();
    /// </code>
    /// </example>
    public class GetDatabaseListTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "GETDBLIST";
        public override string TaskName => $"Get names of all databases";
        public override void Execute()
        {
            if (ConnectionType == ConnectionManagerType.SQLite)
                throw new ETLBoxNotSupportedException("This task is not supported with SQLite!");

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


        public List<string> DatabaseNames { get; set; }
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

        public static List<string> List()
            => new GetDatabaseListTask().GetList().DatabaseNames;
        public static List<string> List(IConnectionManager connectionManager)
            => new GetDatabaseListTask() { ConnectionManager = connectionManager }.GetList().DatabaseNames;

    }
}
