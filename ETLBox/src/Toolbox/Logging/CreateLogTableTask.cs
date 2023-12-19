using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;

namespace ALE.ETLBox.Logging
{
    /// <summary>
    /// Will create the default log table for the default database logging
    /// </summary>
    [PublicAPI]
    public sealed class CreateLogTableTask : GenericTask
    {
        /* ITask Interface */
        public override string TaskName => "Create default etlbox log table";
        public string LogTableName { get; set; }
        public string Sql => LogTable.Sql;
        public CreateTableTask LogTable { get; private set; }

        public void Execute()
        {
            InitCreateTableTask();

            LogTable.CopyTaskProperties(this);
            LogTable.DisableLogging = true;
            LogTable.Create();
            ControlFlow.ControlFlow.LogTable = LogTableName;
        }

        public CreateLogTableTask(string logTableName)
        {
            LogTableName = logTableName;
        }

        public CreateLogTableTask(IConnectionManager connectionManager, string logTableName)
            : this(logTableName)
        {
            ConnectionManager = connectionManager;
        }

        private void InitCreateTableTask()
        {
            var columns = new List<TableColumn>
            {
                new("id", GetIdentityType(), allowNulls: false, isPrimaryKey: true, isIdentity: true),
                new("log_date", "DATETIME", allowNulls: false),
                new("level", "VARCHAR(10)", allowNulls: true),
                new("stage", "VARCHAR(20)", allowNulls: true),
                new("message", "VARCHAR(4000)", allowNulls: true),
                new("task_type", "VARCHAR(200)", allowNulls: true),
                new("task_action", "VARCHAR(5)", allowNulls: true),
                new("task_hash", "CHAR(40)", allowNulls: true),
                new("source", "VARCHAR(20)", allowNulls: true),
                new("load_process_id", "BIGINT", allowNulls: true)
            };
            LogTable = new CreateTableTask(LogTableName, columns);
        }

        private string GetIdentityType()
          => ConnectionType switch
          {
              ConnectionManagerType.ClickHouse => "UUID",
              _ => "BIGINT"
          };

        public static void Create(
            string logTableName = ControlFlow.ControlFlow.DefaultLogTableName
        ) => new CreateLogTableTask(logTableName).Execute();

        public static void Create(
            IConnectionManager connectionManager,
            string logTableName = ControlFlow.ControlFlow.DefaultLogTableName
        ) => new CreateLogTableTask(connectionManager, logTableName).Execute();
    }
}
