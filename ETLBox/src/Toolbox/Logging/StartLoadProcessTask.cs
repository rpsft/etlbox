using ALE.ETLBox.Common;
using ALE.ETLBox.Common.ControlFlow;
using ALE.ETLBox.ControlFlow;
using ETLBox.Primitives;

namespace ALE.ETLBox.Logging
{
    /// <summary>
    /// Starts a load process.
    /// </summary>
    [PublicAPI]
    public class StartLoadProcessTask : GenericTask
    {
        /* ITask Interface */
        public override string TaskName => $"Start load process {ProcessName}";

        private void Execute()
        {
            new SqlTask(this, insertSql)
            {
                Parameter = new List<QueryParameter>
                {
                    new("CurrentDate", "DATETIME", DateTime.Now),
                    new("ProcessName", "VARCHAR(100)", ProcessName),
                    new("StartMessage", "VARCHAR(4000)", StartMessage),
                    new("Source", "VARCHAR(20)", Source)
                },
                DisableLogging = true
            }.ExecuteNonQuery();
            LoadProcessId = new SqlTask(this, LastIdSql).ExecuteScalar<long>();
            var tableTask = new ReadLoadProcessTableTask(this, LoadProcessId)
            {
                DisableLogging = true
            };
            tableTask.Execute();
            Common.ControlFlow.ControlFlow.CurrentLoadProcess = tableTask.LoadProcess;
        }

        /* Public properties */
        public string ProcessName { get; set; } = "N/A";
        public string StartMessage { get; set; }
        public string Source { get; set; } = "ETL";

        private long? _loadProcessId;
        public long? LoadProcessId
        {
            get { return _loadProcessId ?? Common.ControlFlow.ControlFlow.CurrentLoadProcess?.Id; }
            set { _loadProcessId = value; }
        }

        private string insertSql =>
            $@"
 INSERT INTO {TN.QuotedFullName} 
( {QB}start_date{QE}, {QB}process_name{QE}, {QB}start_message{QE}, {QB}source{QE}, {QB}is_running{QE})
 VALUES (@CurrentDate,@ProcessName, @StartMessage,@Source, 1 )";

        private ObjectNameDescriptor TN =>
            new(Common.ControlFlow.ControlFlow.LoadProcessTable, QB, QE);

        private string LastIdSql => $"SELECT MAX({QB}id{QE}) FROM {TN.QuotedFullName}";

        public StartLoadProcessTask() { }

        public StartLoadProcessTask(string processName)
            : this()
        {
            ProcessName = processName;
        }

        public StartLoadProcessTask(string processName, string startMessage)
            : this(processName)
        {
            StartMessage = startMessage;
        }

        public StartLoadProcessTask(string processName, string startMessage, string source)
            : this(processName, startMessage)
        {
            Source = source;
        }

        public static void Start(string processName) =>
            new StartLoadProcessTask(processName).Execute();

        public static void Start(string processName, string startMessage) =>
            new StartLoadProcessTask(processName, startMessage).Execute();

        public static void Start(string processName, string startMessage, string source) =>
            new StartLoadProcessTask(processName, startMessage, source).Execute();

        public static void Start(IConnectionManager connectionManager, string processName) =>
            new StartLoadProcessTask(processName)
            {
                ConnectionManager = connectionManager
            }.Execute();

        public static void Start(
            IConnectionManager connectionManager,
            string processName,
            string startMessage
        ) =>
            new StartLoadProcessTask(processName, startMessage)
            {
                ConnectionManager = connectionManager
            }.Execute();

        public static void Start(
            IConnectionManager connectionManager,
            string processName,
            string startMessage,
            string source
        ) =>
            new StartLoadProcessTask(processName, startMessage, source)
            {
                ConnectionManager = connectionManager
            }.Execute();
    }
}
