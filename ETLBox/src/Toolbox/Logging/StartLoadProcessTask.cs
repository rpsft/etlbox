using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.Helper;
using System;
using System.Collections.Generic;

namespace ETLBox.Logging
{
    /// <summary>
    /// Starts a load process.
    /// </summary>
    public class StartLoadProcessTask : ControlFlowTask
    {
        /* ITask Interface */
        public override string TaskName => $"Start load process {ProcessName}";
        public void Execute()
        {
            QueryParameter cd = new QueryParameter("CurrentDate", "DATETIME", DateTime.Now);
            QueryParameter pn = new QueryParameter("ProcessName", "VARCHAR(100)", ProcessName);
            QueryParameter sm = new QueryParameter("StartMessage", "VARCHAR(4000)", StartMessage);
            QueryParameter so = new QueryParameter("Source", "VARCHAR(20)", Source);
            if (ConnectionType == ConnectionManagerType.Postgres ||
                ConnectionType == ConnectionManagerType.SqlServer ||
                ConnectionType == ConnectionManagerType.MySql)
            {
                LoadProcessId = new SqlTask(this, Sql)
                {
                    Parameter = new List<QueryParameter>() { cd, pn, sm, so },
                    DisableLogging = true,
                }.ExecuteScalar<long>();
            }
            else
            {
                new SqlTask(this, Sql)
                {
                    Parameter = new List<QueryParameter>() { cd, pn, sm, so },
                    DisableLogging = true,
                }.ExecuteNonQuery();
                LoadProcessId = new SqlTask(this, MaxIdSql)
                {
                    DisableLogging = true,
                }.ExecuteScalar<long>();
            }
            var rlp = new ReadLoadProcessTableTask(this, LoadProcessId)
            {
                DisableLogging = true
            };
            rlp.Execute();
            Logging.CurrentLoadProcess = rlp.LoadProcess;
        }

        /* Public properties */
        public string ProcessName { get; set; } = "N/A";
        public string StartMessage { get; set; }
        public string Source { get; set; } = "ETL";

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

        string PP => this.DbConnectionManager?.PP;

        public string Sql => $@"
 INSERT INTO { TN.QuotatedFullName } 
( {QB}start_date{QE}, {QB}process_name{QE}, {QB}start_message{QE}, {QB}source{QE}, {QB}is_running{QE})
 VALUES ({PP}CurrentDate,{PP}ProcessName, {PP}StartMessage,{PP}Source, 1 )
{LastIdSql}";

        ObjectNameDescriptor TN => new ObjectNameDescriptor(Logging.LoadProcessTable, QB, QE);

        string LastIdSql
        {
            get
            {
                if (ConnectionType == ConnectionManagerType.Postgres)
                    return $"RETURNING {QB}id{QE}";
                else if (ConnectionType == ConnectionManagerType.SqlServer)
                    return $"SELECT CAST ( SCOPE_IDENTITY() AS BIGINT)";
                else if (ConnectionType == ConnectionManagerType.MySql)
                    return "; SELECT LAST_INSERT_ID()";
                else
                    return string.Empty;
            }
        }

        string MaxIdSql => $"SELECT MAX({QB}id{QE}) FROM {TN.QuotatedFullName}";

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
