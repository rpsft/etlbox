﻿using ALE.ETLBox.Common;
using ALE.ETLBox.Common.ControlFlow;
using ALE.ETLBox.ControlFlow;
using ETLBox.Primitives;

namespace ALE.ETLBox.Logging
{
    /// <summary>
    /// Will create the default log table for the default database logging
    /// You can use `ControlFlow.SetLoggingDatabase(IConnectionManager connectionManager, string logTableName) to let ETLBox
    /// update your nlog.config add add this table as database target automatically.
    /// Or you can update your nlog.config manually.
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
            LogTable.CopyTaskProperties(this);
            LogTable.DisableLogging = true;
            LogTable.Create();
            Common.ControlFlow.ControlFlow.LogTable = LogTableName;
        }

        public CreateLogTableTask(string logTableName)
        {
            LogTableName = logTableName;
            InitCreateTableTask();
        }

        public CreateLogTableTask(IConnectionManager connectionManager, string logTableName)
            : this(logTableName)
        {
            ConnectionManager = connectionManager;
        }

        private void InitCreateTableTask()
        {
            List<TableColumn> columns = new List<TableColumn>
            {
                new("id", "BIGINT", allowNulls: false, isPrimaryKey: true, isIdentity: true),
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

        public static void Create(
            string logTableName = Common.ControlFlow.ControlFlow.DefaultLogTableName
        ) => new CreateLogTableTask(logTableName).Execute();

        public static void Create(
            IConnectionManager connectionManager,
            string logTableName = Common.ControlFlow.ControlFlow.DefaultLogTableName
        ) => new CreateLogTableTask(connectionManager, logTableName).Execute();
    }
}
