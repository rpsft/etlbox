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
    public class GetListTask : ControlFlowTask
    {
        public override string TaskName { get; set; } = $"Get a list of database objects";

        /// <summary>
        /// A list containing all databases after executing.
        /// </summary>
        public List<ObjectNameDescriptor> ObjectNames { get; set; }


        public GetListTask()
        {

        }

        public GetListTask RetrieveAll()
        {
            Execute();
            return this;
        }


        public string Sql
        {
            get
            {
                return GetSql();
            }
        }

        internal void Execute()
        {

            ObjectNames = new List<ObjectNameDescriptor>();
            new SqlTask(this, Sql)
            {
                Actions = new List<Action<object>>() {
                    name => ObjectNames.Add(new ObjectNameDescriptor((string)name, this.DbConnectionManager.QB, this.DbConnectionManager.QE))
                }
            }.ExecuteReader();

            CleanUpRetrievedList();
        }

        internal virtual string GetSql()
        {
            return string.Empty;
        }

        internal virtual void CleanUpRetrievedList()
        {

        }


    }
}
