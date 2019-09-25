using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;

namespace ALE.ETLBox.Logging
{
    /// <summary>
    /// Will set the table entry for current load process to aborted.
    /// </summary>
    public class AbortLoadProcessTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Abort process with key {LoadProcessKey}";
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
        public string AbortMessage { get; set; }


        public string Sql => $@"EXECUTE etl.AbortLoadProcess
	 @LoadProcessKey = '{LoadProcessKey ?? ControlFlow.ControlFlow.CurrentLoadProcess.LoadProcessKey}',
	 @AbortMessage = {AbortMessage.NullOrSqlString()}";

        public AbortLoadProcessTask()
        {

        }

        public AbortLoadProcessTask(int? loadProcessKey) : this()
        {
            this.LoadProcessKey = loadProcessKey;
        }
        public AbortLoadProcessTask(int? loadProcessKey, string abortMessage) : this(loadProcessKey)
        {
            this.AbortMessage = abortMessage;
        }

        public AbortLoadProcessTask(string abortMessage) : this()
        {
            this.AbortMessage = abortMessage;
        }

        public static void Abort() => new AbortLoadProcessTask().Execute();
        public static void Abort(int? loadProcessKey) => new AbortLoadProcessTask(loadProcessKey).Execute();
        public static void Abort(string abortMessage) => new AbortLoadProcessTask(abortMessage).Execute();
        public static void Abort(int? loadProcessKey, string abortMessage) => new AbortLoadProcessTask(loadProcessKey, abortMessage).Execute();
        public static void Abort(IConnectionManager connectionManager)
            => new AbortLoadProcessTask() { ConnectionManager = connectionManager }.Execute();
        public static void Abort(IConnectionManager connectionManager,int? loadProcessKey)
            => new AbortLoadProcessTask(loadProcessKey) { ConnectionManager = connectionManager }.Execute();
        public static void Abort(IConnectionManager connectionManager, string abortMessage)
            => new AbortLoadProcessTask(abortMessage) { ConnectionManager = connectionManager }.Execute();
        public static void Abort(IConnectionManager connectionManager, int? loadProcessKey, string abortMessage)
            => new AbortLoadProcessTask(loadProcessKey, abortMessage) { ConnectionManager = connectionManager }.Execute();


    }
}
