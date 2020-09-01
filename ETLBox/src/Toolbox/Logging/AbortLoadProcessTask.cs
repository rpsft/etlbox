using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.Helper;
using System;
using System.Collections.Generic;

namespace ETLBox.Logging
{
    /// <summary>
    /// Will set the table entry for current load process to aborted.
    /// </summary>
    public class AbortLoadProcessTask : ControlFlowTask
    {
        /* ITask Interface */
        public override string TaskName => $"Abort process with key {LoadProcessId}";
        public void Execute()
        {
            QueryParameter cd = new QueryParameter("CurrentDate", "DATETIME", DateTime.Now);
            QueryParameter em = new QueryParameter("AbortMessage", "VARCHAR(100)", AbortMessage);
            QueryParameter lpk = new QueryParameter("LoadProcessId", "BIGINT", LoadProcessId);
            new SqlTask(this, Sql)
            {
                DisableLogging = true,
                Parameter = new List<QueryParameter>() { cd, em, lpk },
            }.ExecuteNonQuery();
            var rlp = new ReadLoadProcessTableTask(this, LoadProcessId)
            {
                DisableLogging = true,
            };
            rlp.Execute();
            Logging.CurrentLoadProcess = rlp.LoadProcess;
        }

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
        public string AbortMessage { get; set; }
        string PP => this.DbConnectionManager?.PP;

        public string Sql => $@"
 UPDATE { TN.QuotatedFullName } 
  SET {QB}end_date{QE} = {PP}CurrentDate
  , {QB}is_running{QE} = 0
  , {QB}was_successful{QE} = 0
  , {QB}was_aborted{QE} = 1
  , {QB}abort_message{QE} = {PP}AbortMessage
  WHERE {QB}id{QE} = {PP}LoadProcessId
";
        ObjectNameDescriptor TN => new ObjectNameDescriptor(Logging.LoadProcessTable, QB, QE);

        public AbortLoadProcessTask()
        {
        }

        public AbortLoadProcessTask(long? loadProcessId) : this()
        {
            this.LoadProcessId = loadProcessId;
        }
        public AbortLoadProcessTask(long? loadProcessId, string abortMessage) : this(loadProcessId)
        {
            this.AbortMessage = abortMessage;
        }

        public AbortLoadProcessTask(string abortMessage) : this()
        {
            this.AbortMessage = abortMessage;
        }

        public static void Abort() => new AbortLoadProcessTask().Execute();
        public static void Abort(long? loadProcessId) => new AbortLoadProcessTask(loadProcessId).Execute();
        public static void Abort(string abortMessage) => new AbortLoadProcessTask(abortMessage).Execute();
        public static void Abort(long? loadProcessId, string abortMessage) => new AbortLoadProcessTask(loadProcessId, abortMessage).Execute();
        public static void Abort(IConnectionManager connectionManager)
            => new AbortLoadProcessTask() { ConnectionManager = connectionManager }.Execute();
        public static void Abort(IConnectionManager connectionManager, long? loadProcessId)
            => new AbortLoadProcessTask(loadProcessId) { ConnectionManager = connectionManager }.Execute();
        public static void Abort(IConnectionManager connectionManager, string abortMessage)
            => new AbortLoadProcessTask(abortMessage) { ConnectionManager = connectionManager }.Execute();
        public static void Abort(IConnectionManager connectionManager, long? loadProcessId, string abortMessage)
            => new AbortLoadProcessTask(loadProcessId, abortMessage) { ConnectionManager = connectionManager }.Execute();


    }
}
