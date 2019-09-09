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
        public override string TaskType { get; set; } = "CREATELOG";
        public override string TaskName => $"Create log tables";
        public override void Execute()
        {
            ExecuteTasks();
        }

        public CreateLogTablesTask()
        {
            CreateETLSchema();
            CreateETLLogTable();
            CreateLoadProcessTable();
            CreateStartProcessProcedure();
            CreateTransferCompletedProcedure();
            CreateEndProcessProcedure();
            CreateAbortProcessProcedure();
        }

        public CreateLogTablesTask(IConnectionManager connectionManager) : this()
        {
            this.ConnectionManager = connectionManager;
        }

        private void CreateETLSchema()
        {
            EtlSchema = new CreateSchemaTask("etl") { DisableLogging = true };
        }

        private void CreateETLLogTable()
        {
            List<ITableColumn> columns = new List<ITableColumn>() {
                new TableColumn("LogKey","int", allowNulls: false, isPrimaryKey: true, isIdentity:true),
                new TableColumn("LogDate","datetime", allowNulls: false),
                new TableColumn("Level","nvarchar(10)", allowNulls: true),
                new TableColumn("Stage","nvarchar(20)", allowNulls: true),
                new TableColumn("Message","nvarchar(4000)", allowNulls: true),
                new TableColumn("TaskType","nvarchar(40)", allowNulls: true),
                new TableColumn("TaskAction","nvarchar(5)", allowNulls: true),
                new TableColumn("TaskHash","char(40)", allowNulls: true),
                new TableColumn("Source","nvarchar(20)", allowNulls: true),
                new TableColumn("LoadProcessKey","int", allowNulls: true)
            };
            LogTable = new CreateTableTask("etl.Log", columns) { DisableLogging = true };
        }

        private void CreateLoadProcessTable()
        {
            List<ITableColumn> lpColumns = new List<ITableColumn>() {
                new TableColumn("LoadProcessKey","int", allowNulls: false, isPrimaryKey: true, isIdentity:true),
                new TableColumn("StartDate","datetime", allowNulls: false),
                new TableColumn("TransferCompletedDate","datetime", allowNulls: true),
                new TableColumn("EndDate","datetime", allowNulls: true),
                new TableColumn("Source","nvarchar(20)", allowNulls: true),
                new TableColumn("ProcessName","nvarchar(100)", allowNulls: false) { DefaultValue = "N/A" },
                new TableColumn("StartMessage","nvarchar(4000)", allowNulls: true)  ,
                new TableColumn("IsRunning","bit", allowNulls: false) { DefaultValue = "1" },
                new TableColumn("EndMessage","nvarchar(4000)", allowNulls: true)  ,
                new TableColumn("WasSuccessful","bit", allowNulls: false) { DefaultValue = "0" },
                new TableColumn("AbortMessage","nvarchar(4000)", allowNulls: true) ,
                new TableColumn("WasAborted","bit", allowNulls: false) { DefaultValue = "0" },
                new TableColumn() { Name= "IsFinished", ComputedColumn = "case when EndDate is not null then cast(1 as bit) else cast(0 as bit) end" },
                new TableColumn() { Name= "IsTransferCompleted", ComputedColumn = "case when TransferCompletedDate is not null then cast(1 as bit) else cast(0 as bit) end" },

            };
            LoadProcessTable = new CreateTableTask("etl.LoadProcess", lpColumns) { DisableLogging = true };
        }

        private void CreateStartProcessProcedure()
        {
            StartProcess = new CRUDProcedureTask("etl.StartLoadProcess", $@"-- Create entry in etlLoadProcess
  insert into etl.LoadProcess(StartDate, ProcessName, StartMessage, Source, IsRunning)
  select getdate(),@ProcessName, @StartMessage,@Source, 1 as IsRunning
  select @LoadProcessKey = SCOPE_IDENTITY()"
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
            TransferCompletedForProcess = new CRUDProcedureTask("etl.TransferCompletedForLoadProcess", $@"-- Set transfer completion date in load process
  update etl.LoadProcess
  set TransferCompletedDate = getdate()
  where LoadProcessKey = @LoadProcessKey
  "
             , new List<ProcedureParameter>() {
                    new ProcedureParameter("LoadProcessKey","int")
             })
            { DisableLogging = true };
        }

        private void CreateEndProcessProcedure()
        {
            EndProcess = new CRUDProcedureTask("etl.EndLoadProcess", $@"-- Set entry in etlLoadProcess to completed
  update etl.LoadProcess
  set EndDate = getdate()
  , IsRunning = 0
  , WasSuccessful = 1
  , WasAborted = 0
  , EndMessage = @EndMessage
  where LoadProcessKey = @LoadProcessKey
  "
               , new List<ProcedureParameter>() {
                    new ProcedureParameter("LoadProcessKey","int"),
                    new ProcedureParameter("EndMessage","nvarchar(4000)",""),
               })
            { DisableLogging = true };
        }

        private void CreateAbortProcessProcedure()
        {
            AbortProcess = new CRUDProcedureTask("etl.AbortLoadProcess", $@"-- Set entry in etlLoadProcess to aborted
  update etl.LoadProcess
  set EndDate = getdate()
  , IsRunning = 0
  , WasSuccessful = 0
  , WasAborted = 1
  , AbortMessage = @AbortMessage
  where LoadProcessKey = @LoadProcessKey
  "
              , new List<ProcedureParameter>() {
                    new ProcedureParameter("LoadProcessKey","int"),
                    new ProcedureParameter("AbortMessage","nvarchar(4000)",""),
              })
            { DisableLogging = true };
        }


        public static void CreateLog() => new CreateLogTablesTask().Execute();
        public static void CreateLog(IConnectionManager connectionManager) => new CreateLogTablesTask(connectionManager).Execute();
        public string Sql => EtlSchema.Sql + Environment.NewLine +
                             LoadProcessTable.Sql + Environment.NewLine +
                             LogTable.Sql + Environment.NewLine +
                             StartProcess.Sql + Environment.NewLine +
                             EndProcess.Sql + Environment.NewLine +
                             AbortProcess.Sql + Environment.NewLine +
                             TransferCompletedForProcess.Sql + Environment.NewLine
            ;

        private void ExecuteTasks()
        {
            EtlSchema.ConnectionManager = this.ConnectionManager;
            LogTable.ConnectionManager = this.ConnectionManager;
            LoadProcessTable.ConnectionManager = this.ConnectionManager;
            StartProcess.ConnectionManager = this.ConnectionManager;
            EndProcess.ConnectionManager = this.ConnectionManager;
            AbortProcess.ConnectionManager = this.ConnectionManager;
            TransferCompletedForProcess.ConnectionManager = this.ConnectionManager;
            EtlSchema.Execute();
            LogTable.Execute();
            LoadProcessTable.Execute();
            StartProcess.Execute();
            EndProcess.Execute();
            AbortProcess.Execute();
            TransferCompletedForProcess.Execute();
        }

        CreateTableTask LogTable { get; set; }
        CreateTableTask LoadProcessTable { get; set; }
        CreateSchemaTask EtlSchema { get; set; }
        CRUDProcedureTask StartProcess { get; set; }
        CRUDProcedureTask EndProcess { get; set; }
        CRUDProcedureTask AbortProcess { get; set; }
        CRUDProcedureTask TransferCompletedForProcess { get; set; }
    }
}
