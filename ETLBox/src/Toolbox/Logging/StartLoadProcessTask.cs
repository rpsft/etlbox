using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;

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
            LoadProcessKey = new SqlTask(this, Sql)
            {
                DisableLogging = true,
                ConnectionManager = this.ConnectionManager
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
        public string Source { get; set; }

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
 DECLARE @LoadProcessKey INT  
 EXECUTE etl.StartLoadProcess
	 @ProcessName = '{ProcessName}',
	 @StartMessage = {StartMessage.NullOrSqlString()},
     @Source = {Source.NullOrSqlString()},
     @LoadProcessKey = @LoadProcessKey OUTPUT
 SELECT	@LoadProcessKey";


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
