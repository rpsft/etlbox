using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using System.Collections.Generic;

namespace ETLBox.Logging
{
    /// <summary>
    /// Will create the default log table for the default database logging
    /// You can use `ControlFlow.SetLoggingDatabase(IConnectionManager connectionManager, string logTableName) to let ETLBox
    /// update your nlog.config add add this table as database target automatically.
    /// Or you can update your nlog.config manually.
    /// </summary>
    public class CreateLogTableTask : ControlFlowTask
    {
        /* ITask Interface */
        public override string TaskName => $"Create default etlbox log table";
        public string LogTableName { get; set; } = Logging.DEFAULTLOGTABLENAME;
        public string Sql => LogTable.Sql;
        public CreateTableTask LogTable { get; private set; }
        public void Execute()
        {
            LogTable.CopyLogTaskProperties(this);
            LogTable.ConnectionManager = this.ConnectionManager;
            LogTable.DisableLogging = true;
            LogTable.Create();
            Logging.LogTable = LogTableName;
        }

        public CreateLogTableTask(string logTableName)
        {
            this.LogTableName = logTableName;
            InitCreateTableTask();
        }

        public CreateLogTableTask(IConnectionManager connectionManager, string logTableName) : this(logTableName)
        {
            this.ConnectionManager = connectionManager;
        }

        private void InitCreateTableTask()
        {
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("id","BIGINT", allowNulls: false, isPrimaryKey: true, isIdentity:true),
                new TableColumn("log_date","DATETIME", allowNulls: false),
                new TableColumn("level","VARCHAR(10)", allowNulls: true),
                new TableColumn("stage","VARCHAR(20)", allowNulls: true),
                new TableColumn("message","VARCHAR(4000)", allowNulls: true),
                new TableColumn("task_type","VARCHAR(200)", allowNulls: true),
                new TableColumn("task_action","VARCHAR(5)", allowNulls: true),
                new TableColumn("task_hash","CHAR(40)", allowNulls: true),
                new TableColumn("source","VARCHAR(20)", allowNulls: true),
                new TableColumn("load_process_id","BIGINT", allowNulls: true)
            };
            LogTable = new CreateTableTask(LogTableName, columns);
        }

        public static void Create(string logTableName = Logging.DEFAULTLOGTABLENAME) => new CreateLogTableTask(logTableName).Execute();
        public static void Create(IConnectionManager connectionManager, string logTableName = Logging.DEFAULTLOGTABLENAME) => new CreateLogTableTask(connectionManager, logTableName).Execute();

    }
}
