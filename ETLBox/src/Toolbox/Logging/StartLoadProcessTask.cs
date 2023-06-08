using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;

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
            QueryParameter cd = new QueryParameter("CurrentDate", "DATETIME", DateTime.Now);
            QueryParameter pn = new QueryParameter("ProcessName", "VARCHAR(100)", ProcessName);
            QueryParameter sm = new QueryParameter("StartMessage", "VARCHAR(4000)", StartMessage);
            QueryParameter so = new QueryParameter("Source", "VARCHAR(20)", Source);
            LoadProcessId = new SqlTask(this, Sql)
            {
                Parameter = new List<QueryParameter> { cd, pn, sm, so },
                DisableLogging = true,
            }.ExecuteScalar<long>();
            var rlp = new ReadLoadProcessTableTask(this, LoadProcessId) { DisableLogging = true };
            rlp.Execute();
            ControlFlow.ControlFlow.CurrentLoadProcess = rlp.LoadProcess;
        }

        /* Public properties */
        public string ProcessName { get; set; } = "N/A";
        public string StartMessage { get; set; }
        public string Source { get; set; } = "ETL";

        private long? _loadProcessId;
        public long? LoadProcessId
        {
            get { return _loadProcessId ?? ControlFlow.ControlFlow.CurrentLoadProcess?.Id; }
            set { _loadProcessId = value; }
        }

        public string Sql =>
            $@"
 INSERT INTO {TN.QuotatedFullName} 
( {QB}start_date{QE}, {QB}process_name{QE}, {QB}start_message{QE}, {QB}source{QE}, {QB}is_running{QE})
 VALUES (@CurrentDate,@ProcessName, @StartMessage,@Source, 1 ) 
{LastIdSql}";

        private ObjectNameDescriptor TN => new(ControlFlow.ControlFlow.LoadProcessTable, QB, QE);

        private string LastIdSql
        {
            get
            {
                if (ConnectionType == ConnectionManagerType.Postgres)
                    return "RETURNING id";
                if (ConnectionType == ConnectionManagerType.SqlServer)
                    return "SELECT CAST ( SCOPE_IDENTITY() AS BIGINT)";
                //else if (ConnectionType == ConnectionManagerType.MySql)
                //    return "; SELECT LAST_INSERT_ID();";
                return $"; SELECT MAX({QB}id{QE}) FROM {TN.QuotatedFullName}";
            }
        }

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
