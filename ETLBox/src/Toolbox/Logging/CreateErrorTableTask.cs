using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;

namespace ALE.ETLBox.Logging
{
    /// <summary>
    /// This task will create a table that can store exceptions (and information about the affected records)
    /// that occur during a data flow execution
    /// </summary>
    public class CreateErrorTableTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Create error table";

        public string TableName { get; set; }
        //public ObjectNameDescriptor ON => new ObjectNameDescriptor(TableName, ConnectionType);

        public void Execute()
        {
            CreateTableTask.Create(this.ConnectionManager, TableName,
                new List<TableColumn>()
                {
                    new TableColumn("ErrorText", "NVARCHAR(MAX)", allowNulls:false),
                    new TableColumn("RecordAsJson", "NVARCHAR(MAX)", allowNulls:true),
                    new TableColumn("ReportTime", "DATETIME", allowNulls:false),
                });
        }

        public CreateErrorTableTask()
        {

        }

        public CreateErrorTableTask(string tableName)
        {
            this.TableName = tableName;
        }

        public CreateErrorTableTask(IConnectionManager connectionManager, string tableName) : this(tableName)
        {
            this.ConnectionManager = connectionManager;
        }

        public static void Create(IConnectionManager connectionManager, string tableName)
            => new CreateErrorTableTask(connectionManager, tableName).Execute();

        public static void Create(string tableName)
            => new CreateErrorTableTask(tableName).Execute();



    }
}
