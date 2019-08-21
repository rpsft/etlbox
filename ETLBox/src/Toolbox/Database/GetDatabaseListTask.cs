using ALE.ETLBox.ConnectionManager;
using System;
using System.Collections.Generic;

namespace ALE.ETLBox.ControlFlow {
    /// <summary>
    /// Returns a list of all databases on the server (make sure to connect with the correct permissions).
    /// </summary>
    /// <example>
    /// <code>
    /// GetDatabaseListTask.List();
    /// </code>
    /// </example>
    public class GetDatabaseListTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "GETDBLIST";
        public override string TaskName => $"Get names of all databases";
        public override void Execute() {
            DatabaseNames = new List<string>();
            new SqlTask(this, Sql) {
                Actions = new List<Action<object>>() {
                    n => DatabaseNames.Add((string)n)
                }
            }.ExecuteReader();
        }


        public List<string> DatabaseNames { get; set; }
        public string Sql {
            get {
                return $"SELECT [name] FROM master.dbo.sysdatabases WHERE dbid > 4";
            }
        }

        public GetDatabaseListTask() {

        }

        public GetDatabaseListTask GetList() {
            Execute();
            return this;
        }

        public static List<string> List()
            => new GetDatabaseListTask().GetList().DatabaseNames;
        public static List<string> List(IConnectionManager connectionManager)
            => new GetDatabaseListTask() { ConnectionManager = connectionManager }.GetList().DatabaseNames;

    }
}
