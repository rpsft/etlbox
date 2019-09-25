using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;

namespace ALE.ETLBox.Logging
{
    /// <summary>
    /// Will set the table entry for current load process to ended.
    /// </summary>
    public class EndLoadProcessTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"End process with key {LoadProcessKey}";
        public override void Execute()
        {
            new SqlTask(this, Sql)
            {
                DisableLogging = true,
                ConnectionManager = this.ConnectionManager
            }.ExecuteNonQuery();
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
        public string EndMessage { get; set; }


        public string Sql => $@"EXECUTE etl.EndLoadProcess
	 @LoadProcessKey = '{LoadProcessKey}',
	 @EndMessage = {EndMessage.NullOrSqlString()}";

        public EndLoadProcessTask()
        {

        }

        public EndLoadProcessTask(int? loadProcessKey) : this()
        {
            this.LoadProcessKey = loadProcessKey;
        }
        public EndLoadProcessTask(int? loadProcessKey, string endMessage) : this(loadProcessKey)
        {
            this.EndMessage = endMessage;
        }
        public EndLoadProcessTask(string endMessage) : this(null, endMessage) { }

        public static void End() => new EndLoadProcessTask().Execute();
        public static void End(int? loadProcessKey) => new EndLoadProcessTask(loadProcessKey).Execute();
        public static void End(int? loadProcessKey, string endMessage) => new EndLoadProcessTask(loadProcessKey, endMessage).Execute();
        public static void End(string endMessage) => new EndLoadProcessTask(null, endMessage).Execute();
        public static void End(IConnectionManager connectionManager)
            => new EndLoadProcessTask() { ConnectionManager = connectionManager }.Execute();
        public static void End(IConnectionManager connectionManager, int? loadProcessKey)
            => new EndLoadProcessTask(loadProcessKey) { ConnectionManager = connectionManager }.Execute();
        public static void End(IConnectionManager connectionManager, int? loadProcessKey, string endMessage)
            => new EndLoadProcessTask(loadProcessKey, endMessage) { ConnectionManager = connectionManager }.Execute();
        public static void End(IConnectionManager connectionManager, string endMessage)
            => new EndLoadProcessTask(null, endMessage) { ConnectionManager = connectionManager }.Execute();


    }
}
