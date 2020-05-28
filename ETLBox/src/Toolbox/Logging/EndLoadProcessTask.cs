using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;

namespace ALE.ETLBox.Logging
{
    /// <summary>
    /// Will set the table entry for current load process to ended.
    /// </summary>
    public class EndLoadProcessTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"End process with key {LoadProcessId}";
        public void Execute()
        {
            QueryParameter cd = new QueryParameter("CurrentDate", "DATETIME", DateTime.Now);
            QueryParameter em = new QueryParameter("EndMessage", "VARCHAR(100)", EndMessage);
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
            ControlFlow.ControlFlow.CurrentLoadProcess = rlp.LoadProcess;
        }

        /* Public properties */
        public long? _loadProcessId;
        public long? LoadProcessId
        {
            get
            {
                return _loadProcessId ?? ControlFlow.ControlFlow.CurrentLoadProcess?.Id;
            }
            set
            {
                _loadProcessId = value;
            }
        }
        public string EndMessage { get; set; }

        public string Sql => $@"
 UPDATE { TN.QuotatedFullName } 
  SET end_date = @CurrentDate
  , is_running = 0
  , was_successful = 1
  , was_aborted = 0
  , end_message = @EndMessage
  WHERE id = @LoadProcessId
";

        ObjectNameDescriptor TN => new ObjectNameDescriptor(ControlFlow.ControlFlow.LoadProcessTable, QB, QE);

        public EndLoadProcessTask()
        {
        }

        public EndLoadProcessTask(long? loadProcessId) : this()
        {
            this.LoadProcessId = loadProcessId;
        }
        public EndLoadProcessTask(long? loadProcessId, string endMessage) : this(loadProcessId)
        {
            this.EndMessage = endMessage;
        }
        public EndLoadProcessTask(string endMessage) : this(null, endMessage) { }

        public static void End() => new EndLoadProcessTask().Execute();
        public static void End(long? loadProcessId) => new EndLoadProcessTask(loadProcessId).Execute();
        public static void End(long? loadProcessId, string endMessage) => new EndLoadProcessTask(loadProcessId, endMessage).Execute();
        public static void End(string endMessage) => new EndLoadProcessTask(null, endMessage).Execute();
        public static void End(IConnectionManager connectionManager)
            => new EndLoadProcessTask() { ConnectionManager = connectionManager }.Execute();
        public static void End(IConnectionManager connectionManager, long? loadProcessId)
            => new EndLoadProcessTask(loadProcessId) { ConnectionManager = connectionManager }.Execute();
        public static void End(IConnectionManager connectionManager, long? loadProcessId, string endMessage)
            => new EndLoadProcessTask(loadProcessId, endMessage) { ConnectionManager = connectionManager }.Execute();
        public static void End(IConnectionManager connectionManager, string endMessage)
            => new EndLoadProcessTask(null, endMessage) { ConnectionManager = connectionManager }.Execute();
    }
}
