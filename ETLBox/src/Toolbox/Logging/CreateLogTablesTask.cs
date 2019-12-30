using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;

namespace ALE.ETLBox.Logging
{
    /// <summary>
    /// Will create two tables: etl.Log and etl.LoadProcess. Also it will create some procedure for starting, stopping and aborting load processes.
    /// If logging is configured via a NLog config, these tables contain log information from the tasks.
    /// </summary>
    public class CreateLogTablesTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Create log tables";
        public string LogTableName { get; set; }
        public string ProcessTableName { get; set; }
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(LogTableName, this.ConnectionType);
        public ObjectNameDescriptor PN => new ObjectNameDescriptor(ProcessTableName, this.ConnectionType);
        public string QB => TN.QB;
        public string QE => TN.QE;
        public void Execute()
        {
            List<ITableColumn> columns = new List<ITableColumn>() {
                new TableColumn("id","INT", allowNulls: false, isPrimaryKey: true, isIdentity:true),
                new TableColumn("log_date","DATETIME", allowNulls: false),
                new TableColumn("level","NVARCHAR(10)", allowNulls: true),
                new TableColumn("stage","NVARCHAR(20)", allowNulls: true),
                new TableColumn("message","NVARCHAR(4000)", allowNulls: true),
                new TableColumn("task_type","NVARCHAR(200)", allowNulls: true),
                new TableColumn("task_action","NVARCHAR(5)", allowNulls: true),
                new TableColumn("task_hash","CHAR(40)", allowNulls: true),
                new TableColumn("source","NVARCHAR(20)", allowNulls: true),
                new TableColumn("load_process_id","INT", allowNulls: true)
            };
            LogTable = new CreateTableTask(LogTableName, columns)
            { ConnectionManager = this.ConnectionManager, DisableLogging = true };
            LogTable.Create();

            //CreateETLLogTable();
            ////CreateLoadProcessTable();
            //ExecuteTasks();
        }

        public CreateLogTablesTask(string logTableName)
        {
            this.LogTableName = logTableName;
            //CreateStartProcessProcedure();
            //CreateTransferCompletedProcedure();
            //CreateEndProcessProcedure();
            //CreateAbortProcessProcedure();
        }

        public CreateLogTablesTask(IConnectionManager connectionManager, string logTableName) : this(logTableName)
        {
            this.ConnectionManager = connectionManager;
        }

        private void CreateETLLogTable()
        {
            List<ITableColumn> columns = new List<ITableColumn>() {
                new TableColumn("LogKey","INT", allowNulls: false, isPrimaryKey: true, isIdentity:true),
                new TableColumn("LogDate","DATETIME", allowNulls: false),
                new TableColumn("Level","NVARCHAR(10)", allowNulls: true),
                new TableColumn("Stage","NVARCHAR(20)", allowNulls: true),
                new TableColumn("Message","NVARCHAR(4000)", allowNulls: true),
                new TableColumn("TaskType","NVARCHAR(200)", allowNulls: true),
                new TableColumn("TaskAction","NVARCHAR(5)", allowNulls: true),
                new TableColumn("TaskHash","CHAR(40)", allowNulls: true),
                new TableColumn("Source","NVARCHAR(20)", allowNulls: true),
                new TableColumn("LoadProcessKey","INT", allowNulls: true)
            };
            LogTable = new CreateTableTask(LogTableName, columns) { DisableLogging = true };
        }

        private void CreateLoadProcessTable()
        {
            List<ITableColumn> lpColumns = new List<ITableColumn>() {
                new TableColumn("LoadProcessKey","INT", allowNulls: false, isPrimaryKey: true, isIdentity:true),
                new TableColumn("StartDate","DATETIME", allowNulls: false),
                new TableColumn("TransferCompletedDate","DATETIME", allowNulls: true),
                new TableColumn("EndDate","DATETIME", allowNulls: true),
                new TableColumn("Source","NVARCHAR(20)", allowNulls: true),
                new TableColumn("ProcessName","NVARCHAR(100)", allowNulls: false) { DefaultValue = "N/A" },
                new TableColumn("StartMessage","NVARCHAR(4000)", allowNulls: true)  ,
                new TableColumn("IsRunning","BIT", allowNulls: false) { DefaultValue = "1" },
                new TableColumn("EndMessage","NVARCHAR(4000)", allowNulls: true)  ,
                new TableColumn("WasSuccessful","BIT", allowNulls: false) { DefaultValue = "0" },
                new TableColumn("AbortMessage","NVARCHAR(4000)", allowNulls: true) ,
                new TableColumn("WasAborted","BIT", allowNulls: false) { DefaultValue = "0" },
                new TableColumn() { Name= "IsFinished", ComputedColumn = $"CASE WHEN {QB}EndDate{QE} IS NOT NULL THEN CAST(1 as bit) ELSE CAST(0 as bit) END" },
                new TableColumn() { Name= "IsTransferCompleted", ComputedColumn = $"CASE WHEN {QB}TransferCompletedDate{QE} IS NOT NULL THEN CAST(1 as bit) ELSE CAST(0 as bit) END" },

            };
            LoadProcessTable = new CreateTableTask(ProcessTableName, lpColumns) { DisableLogging = true };
        }

        private void CreateStartProcessProcedure()
        {
            StartProcess = new CreateProcedureTask("etl.StartLoadProcess", $@"-- Create entry in etlLoadProcess
  INSERT INTO etl.LoadProcess(StartDate, ProcessName, StartMessage, Source, IsRunning)
  SELECT getdate(),@ProcessName, @StartMessage,@Source, 1 as IsRunning
  SELECT @LoadProcessKey = SCOPE_IDENTITY()"
                , new List<ProcedureParameter>() {
                    new ProcedureParameter("ProcessName","nvarchar(100)"),
                    new ProcedureParameter("StartMessage","nvarchar(4000)",""),
                    new ProcedureParameter("Source","nvarchar(20)",""),
                    new ProcedureParameter("LoadProcessKey","int") { Out = true }
                })
            { DisableLogging = true };
        }

        private void CreateTransferCompletedProcedure()
        {
            TransferCompletedForProcess = new CreateProcedureTask("etl.TransferCompletedForLoadProcess", $@"-- Set transfer completion date in load process
  UPDATE etl.LoadProcess
  SET TransferCompletedDate = getdate()
  WHERE LoadProcessKey = @LoadProcessKey
  "
             , new List<ProcedureParameter>() {
                    new ProcedureParameter("LoadProcessKey","int")
             })
            { DisableLogging = true };
        }

        private void CreateEndProcessProcedure()
        {
            EndProcess = new CreateProcedureTask("etl.EndLoadProcess", $@"-- Set entry in etlLoadProcess to completed
  UPDATE etl.LoadProcess
  SET EndDate = getdate()
  , IsRunning = 0
  , WasSuccessful = 1
  , WasAborted = 0
  , EndMessage = @EndMessage
  WHERE LoadProcessKey = @LoadProcessKey
  "
               , new List<ProcedureParameter>() {
                    new ProcedureParameter("LoadProcessKey","int"),
                    new ProcedureParameter("EndMessage","nvarchar(4000)",""),
               })
            { DisableLogging = true };
        }

        private void CreateAbortProcessProcedure()
        {
            AbortProcess = new CreateProcedureTask("etl.AbortLoadProcess", $@"-- Set entry in etlLoadProcess to aborted
  UPDATE etl.LoadProcess
  SET EndDate = getdate()
  , IsRunning = 0
  , WasSuccessful = 0
  , WasAborted = 1
  , AbortMessage = @AbortMessage
  WHERE LoadProcessKey = @LoadProcessKey
  "
              , new List<ProcedureParameter>() {
                    new ProcedureParameter("LoadProcessKey","int"),
                    new ProcedureParameter("AbortMessage","nvarchar(4000)",""),
              })
            { DisableLogging = true };
        }


        public static void CreateLog(string logTableName) => new CreateLogTablesTask(logTableName).Execute();
        public static void CreateLog(IConnectionManager connectionManager, string logTableName) => new CreateLogTablesTask(connectionManager, logTableName).Execute();
        public string Sql => //EtlSchema.Sql + Environment.NewLine +
                             LoadProcessTable.Sql + Environment.NewLine +
                             LogTable.Sql + Environment.NewLine +
                             StartProcess.Sql + Environment.NewLine +
                             EndProcess.Sql + Environment.NewLine +
                             AbortProcess.Sql + Environment.NewLine +
                             TransferCompletedForProcess.Sql + Environment.NewLine
            ;

        private void ExecuteTasks()
        {
            //EtlSchema.ConnectionManager = this.ConnectionManager;
            LogTable.ConnectionManager = this.ConnectionManager;
            LoadProcessTable.ConnectionManager = this.ConnectionManager;
            //StartProcess.ConnectionManager = this.ConnectionManager;
            //EndProcess.ConnectionManager = this.ConnectionManager;
            //AbortProcess.ConnectionManager = this.ConnectionManager;
            //TransferCompletedForProcess.ConnectionManager = this.ConnectionManager;
            //EtlSchema.Execute();
            LogTable.Execute();
            LoadProcessTable.Execute();
            //StartProcess.Execute();
            //EndProcess.Execute();
            //AbortProcess.Execute();
            //TransferCompletedForProcess.Execute();
        }

        CreateTableTask LogTable { get; set; }
        CreateTableTask LoadProcessTable { get; set; }
        CreateProcedureTask StartProcess { get; set; }
        CreateProcedureTask EndProcess { get; set; }
        CreateProcedureTask AbortProcess { get; set; }
        CreateProcedureTask TransferCompletedForProcess { get; set; }
    }
}
