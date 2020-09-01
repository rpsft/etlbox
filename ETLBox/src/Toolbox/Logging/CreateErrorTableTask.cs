using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using System.Collections.Generic;

namespace ETLBox.Logging
{
    /// <summary>
    /// This task will create a table that can store exceptions (and information about the affected records)
    /// that occur during a data flow execution
    /// </summary>
    public class CreateErrorTableTask : ControlFlowTask
    {
        /* ITask Interface */
        public override string TaskName => $"Create error table";

        public string TableName { get; set; }

        public bool DropAndCreateTable { get; set; }

        public void Execute()
        {
            if (DropAndCreateTable)
                DropTableTask.DropIfExists(this.ConnectionManager, TableName);

            CreateTableTask.Create(this.ConnectionManager, TableName,
                new List<TableColumn>()
                {
                    new TableColumn("ErrorText", "TEXT", allowNulls:false),
                    new TableColumn("ExceptionType", "VARCHAR(1000)", allowNulls:false),
                    new TableColumn("RecordAsJson", "TEXT", allowNulls:true),
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

        public static void DropAndCreate(IConnectionManager connectionManager, string tableName)
    => new CreateErrorTableTask(connectionManager, tableName) { DropAndCreateTable = true }.Execute();

        public static void DropAndCreate(string tableName)
            => new CreateErrorTableTask(tableName) { DropAndCreateTable = true }.Execute();

    }
}
