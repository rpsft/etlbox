using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;

namespace ALE.ETLBox.Logging
{
    /// <summary>
    /// Starts a load process.
    /// </summary>
    public class StartLoadProcessTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Start load process {ProcessName}";
        public void Execute()
        {
            QueryParameter pn = new QueryParameter("ProcessName", "VARCHAR(100)", ProcessName);
            QueryParameter sm = new QueryParameter("StartMessage", "VARCHAR(4000)", StartMessage);
            QueryParameter so = new QueryParameter("Source", "VARCHAR(20)", Source);
            LoadProcessKey = new SqlTask(this, Sql)
            {
                Parameter = new List<QueryParameter>() { pn, sm, so},
                DisableLogging = true,
            }.ExecuteScalar<int>();
            var rlp = new ReadLoadProcessTableTask(LoadProcessKey)
            {
                TaskType = this.TaskType,
                TaskHash = this.TaskHash,
                DisableLogging = true,
                ConnectionManager = this.ConnectionManager
            };
            rlp.Execute();
            ControlFlow.ControlFlow.CurrentLoadProcess = rlp.LoadProcess;
        }

        /* Public properties */
        public string ProcessName { get; set; } = "N/A";
        public string StartMessage { get; set; }
        public string Source { get; set; } = "ETL";

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

        public string Sql => $@"
 INSERT INTO etl.LoadProcess( start_date, process_name, start_message, source, is_running)
 SELECT GETDATE(),@ProcessName, @StartMessage,@Source, 1 as IsRunning";


        public StartLoadProcessTask()
        {

        }
        public StartLoadProcessTask(string processName) : this()
        {
            this.ProcessName = processName;
        }
        public StartLoadProcessTask(string processName, string startMessage) : this(processName)
        {
            this.StartMessage = startMessage;
        }

        public StartLoadProcessTask(string processName, string startMessage, string source) : this(processName, startMessage)
        {
            this.Source = source;
        }

        public static void Start(string processName) => new StartLoadProcessTask(processName).Execute();
        public static void Start(string processName, string startMessage) => new StartLoadProcessTask(processName, startMessage).Execute();
        public static void Start(string processName, string startMessage, string source) => new StartLoadProcessTask(processName, startMessage, source).Execute();
        public static void Start(IConnectionManager connectionManager, string processName)
            => new StartLoadProcessTask(processName) { ConnectionManager = connectionManager }.Execute();
        public static void Start(IConnectionManager connectionManager, string processName, string startMessage)
            => new StartLoadProcessTask(processName, startMessage) { ConnectionManager = connectionManager }.Execute();
        public static void Start(IConnectionManager connectionManager, string processName, string startMessage, string source)
            => new StartLoadProcessTask(processName, startMessage, source) { ConnectionManager = connectionManager }.Execute();


    }
}
