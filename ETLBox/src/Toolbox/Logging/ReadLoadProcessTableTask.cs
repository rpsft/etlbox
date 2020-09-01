using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.Helper;
using System;
using System.Collections.Generic;

namespace ETLBox.Logging
{
    /// <summary>
    /// Read load processes by Id, all processes or last finished/successful/aborted.
    /// </summary>
    public class ReadLoadProcessTableTask : ControlFlowTask
    {
        /* ITask Interface */
        public override string TaskName => $"Read load processes by Id, all processes or last finished/successful/aborted.";
        public void Execute()
        {
            LoadProcess = new LoadProcess();
            var sql = new SqlTask(this, Sql)
            {
                DisableLogging = true,
                Actions = new List<Action<object>>() {
                col => LoadProcess.Id = Convert.ToInt64(col),
                col => LoadProcess.StartDate = (DateTime)col,
                col => LoadProcess.EndDate = (DateTime?)col,
                col => LoadProcess.Source = (string)col,
                col => LoadProcess.ProcessName = (string)col,
                col => LoadProcess.StartMessage = (string)col,
                col => LoadProcess.IsRunning = Convert.ToInt16(col) > 0 ? true : false,
                col => LoadProcess.EndMessage = (string)col,
                col => LoadProcess.WasSuccessful = Convert.ToInt16(col) > 0 ? true : false,
                col => LoadProcess.AbortMessage = (string)col,
                col => LoadProcess.WasAborted= Convert.ToInt16(col) > 0 ? true : false,
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
        long? _loadProcessId;
        public long? LoadProcessId
        {
            get
            {
                return _loadProcessId ?? Logging.CurrentLoadProcess?.Id;
            }
            set
            {
                _loadProcessId = value;
            }
        }
        public LoadProcess LoadProcess { get; private set; }
        public List<LoadProcess> AllLoadProcesses { get; set; }

        public LoadProcess LastFinished { get; private set; }
        public LoadProcess LastTransferred { get; private set; }
        public ReadOptions ReadOption { get; set; } = ReadOptions.ReadSingleProcess;

        public string Sql
        {
            get
            {
                string sql = $@"
SELECT {Top1Sql} {QB}id{QE}, {QB}start_date{QE}, {QB}end_date{QE}, {QB}source{QE}, {QB}process_name{QE}, {QB}start_message{QE}, {QB}is_running{QE}, {QB}end_message{QE}, {QB}was_successful{QE}, {QB}abort_message{QE}, {QB}was_aborted{QE}
FROM {TN.QuotatedFullName} ";
                if (ReadOption == ReadOptions.ReadSingleProcess)
                    sql += $@"WHERE {QB}id{QE} = {LoadProcessId}";
                else if (ReadOption == ReadOptions.ReadLastFinishedProcess)
                    sql += $@"WHERE {QB}was_successful{QE} = 1 || {QB}was_aborted{QE} = 1
ORDER BY {QB}end_date{QE} DESC, {QB}id{QE} DESC";
                else if (ReadOption == ReadOptions.ReadLastSuccessful)
                    sql += $@"WHERE {QB}was_successful{QE} = 1
ORDER BY {QB}end_date{QE} DESC, {QB}id{QE} DESC";
                else if (ReadOption == ReadOptions.ReadLastAborted)
                    sql += $@"WHERE {QB}was_aborted{QE} = 1
ORDER BY {QB}end_date{QE} DESC, {QB}id{QE} DESC";
                sql += Environment.NewLine + Limit1Sql;
                return sql;
            }
        }

        private string Top1Sql
        {
            get
            {
                if (ReadOption == ReadOptions.ReadAllProcesses)
                    return string.Empty;
                else
                {
                    if (ConnectionType == ConnectionManagerType.SqlServer)
                        return "TOP 1";
                    else
                        return string.Empty;
                }
            }
        }

        private string Limit1Sql
        {
            get
            {
                if (ReadOption == ReadOptions.ReadAllProcesses)
                    return string.Empty;
                else
                {
                    if (ConnectionType == ConnectionManagerType.Postgres)
                        return "LIMIT 1";
                    else
                        return string.Empty;
                }
            }
        }

        ObjectNameDescriptor TN => new ObjectNameDescriptor(Logging.LoadProcessTable, QB, QE);

        public ReadLoadProcessTableTask()
        {

        }
        public ReadLoadProcessTableTask(long? loadProcessId) : this()
        {
            this.LoadProcessId = loadProcessId;
        }

        internal ReadLoadProcessTableTask(ControlFlowTask callingTask, long? loadProcessId) : this(loadProcessId)
        {
            this.CopyLogTaskProperties(callingTask);
            this.ConnectionManager = callingTask.ConnectionManager;
        }

        public static LoadProcess Read(long? loadProcessId)
        {
            var sql = new ReadLoadProcessTableTask(loadProcessId);
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
        ReadLastSuccessful,
        ReadLastAborted
    }
}
