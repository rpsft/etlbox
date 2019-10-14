using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;

namespace ALE.ETLBox.Logging
{
    /// <summary>
    /// Reads data from the etl.LoadProcessTable.
    /// </summary>
    public class ReadLoadProcessTableTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Read process with Key ({LoadProcessKey}) or without";
        public void Execute()
        {
            LoadProcess = new LoadProcess();
            var sql = new SqlTask(this, Sql)
            {
                DisableLogging = true,
                DisableExtension = true,
                ConnectionManager = this.ConnectionManager,
                Actions = new List<Action<object>>() {
                col => LoadProcess.LoadProcessKey = (int)col,
                col => LoadProcess.StartDate = (DateTime)col,
                col => LoadProcess.TransferCompletedDate = (DateTime?)col,
                col => LoadProcess.EndDate = (DateTime?)col,
                col => LoadProcess.ProcessName = (string)col,
                col => LoadProcess.StartMessage = (string)col,
                col => LoadProcess.IsRunning = (bool)col,
                col => LoadProcess.EndMessage = (string)col,
                col => LoadProcess.WasSuccessful = (bool)col,
                col => LoadProcess.AbortMessage = (string)col,
                col => LoadProcess.WasAborted= (bool)col,
                col => LoadProcess.IsFinished= (bool)col,
                col => LoadProcess.IsTransferCompleted= (bool)col
                }
            };
            if (ReadOption == ReadOptions.ReadAllProcesses)
            {
                sql.BeforeRowReadAction = () => AllLoadProcesses = new List<LoadProcess>();
                sql.AfterRowReadAction = () => AllLoadProcesses.Add(LoadProcess);
            }
            sql.ExecuteReader();
        }

        /* Public properties */
        public int? _loadProcessKey;
        public int? LoadProcessKey
        {
            get
            {
                return _loadProcessKey ?? ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey;
            }
            set
            {
                _loadProcessKey = value;
            }
        }
        public LoadProcess LoadProcess { get; private set; }
        public List<LoadProcess> AllLoadProcesses { get; set; }

        public LoadProcess LastFinished { get; private set; }
        public LoadProcess LastTransfered { get; private set; }
        public ReadOptions ReadOption { get; set; } = ReadOptions.ReadSingleProcess;

        public string Sql
        {
            get
            {
                string top1 = "";
                if (ReadOption != ReadOptions.ReadAllProcesses)
                    top1 = "top 1";
                string sql = $@"
SELECT {top1} LoadProcessKey, StartDate, TransferCompletedDate, EndDate, ProcessName, StartMessage, IsRunning, EndMessage, WasSuccessful, AbortMessage, WasAborted, IsFinished, IsTransferCompleted
FROM etl.LoadProcess ";
                if (ReadOption == ReadOptions.ReadSingleProcess)
                    sql += $@"WHERE LoadProcessKey = {LoadProcessKey}";
                else if (ReadOption == ReadOptions.ReadLastFinishedProcess)
                    sql += $@"WHERE IsFinished = 1
ORDER BY EndDate desc, LoadProcessKey DESC";
                else if (ReadOption == ReadOptions.ReadLastSuccessful)
                    sql += $@"WHERE WasSuccessful = 1
ORDER BY EndDate desc, LoadProcessKey DESC";
                else if (ReadOption == ReadOptions.ReadLastAborted)
                    sql += $@"WHERE WasAborted = 1
ORDER BY EndDate desc, LoadProcessKey DESC";
                else if (ReadOption == ReadOptions.ReadLastTransferedProcess)
                    sql += $@"WHERE IsTransferCompleted = 1
ORDER BY TransferCompletedDate DESC,
LoadProcessKey DESC";

                return sql;
            }
        }

        public ReadLoadProcessTableTask()
        {

        }
        public ReadLoadProcessTableTask(int? loadProcessKey) : this()
        {
            this.LoadProcessKey = loadProcessKey;
        }

        public static LoadProcess Read(int? loadProcessKey)
        {
            var sql = new ReadLoadProcessTableTask(loadProcessKey);
            sql.Execute();
            return sql.LoadProcess;
        }
        public static List<LoadProcess> ReadAll()
        {
            var sql = new ReadLoadProcessTableTask() { ReadOption = ReadOptions.ReadAllProcesses };
            sql.Execute();
            return sql.AllLoadProcesses;
        }

        public static LoadProcess ReadWithOption(ReadOptions option)
        {
            var sql = new ReadLoadProcessTableTask() { ReadOption = option };
            sql.Execute();
            return sql.LoadProcess;
        }
    }

    public enum ReadOptions
    {
        ReadSingleProcess,
        ReadAllProcesses,
        ReadLastFinishedProcess,
        ReadLastTransferedProcess,
        ReadLastSuccessful,
        ReadLastAborted
    }
}
