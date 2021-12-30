using ETLBox.Connection;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.Tasks;
using ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ETLBox.Logging
{
    /// <summary>
    /// Let you manage load process logging.
    /// You can start, stop and abort load processes with this task. 
    /// It also allows you to create a the required table in your database.
    /// </summary>
    public sealed class LoadProcessTask : ControlFlowTask
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = $"Manages load processes";
        public LoadProcess StartProcess(string startMessage = "") {
            QueryParameter cd = new QueryParameter("CurrentDate", "DATETIME", DateTime.Now);
            QueryParameter pn = new QueryParameter("ProcessName", "VARCHAR(100)", Process.ProcessName);
            QueryParameter sm = new QueryParameter("StartMessage", "VARCHAR(4000)", startMessage);
            QueryParameter so = new QueryParameter("Source", "VARCHAR(20)", Process.Source);
            QueryParameter si = new QueryParameter("SourceId", "BIGINT", Process.SourceId);
            if (ConnectionType == ConnectionManagerType.Postgres ||
                ConnectionType == ConnectionManagerType.SqlServer ||
                ConnectionType == ConnectionManagerType.MySql) {
                Process.Id = new SqlTask(this, Sql_Start) {
                    Parameter = new List<QueryParameter>() { cd, pn, sm, so, si },
                    DisableLogging = true,
                }.ExecuteScalar<long>();
            } else {
                new SqlTask(this, Sql_Start) {
                    Parameter = new List<QueryParameter>() { cd, pn, sm, so, si },
                    DisableLogging = true,
                }.ExecuteNonQuery();
                Process.Id = new SqlTask(this, MaxIdSql) {
                    DisableLogging = true,
                }.ExecuteScalar<long>();
            }
            Process = ReadProcess(Process.Id ?? 0);
            Logging.CurrentLoadProcess = Process;
            return Process;
        }

        public LoadProcess AbortProcess(string abortMessage = "") {
            QueryParameter cd = new QueryParameter("CurrentDate", "DATETIME", DateTime.Now);
            QueryParameter em = new QueryParameter("AbortMessage", "VARCHAR(100)", abortMessage);
            QueryParameter lpk = new QueryParameter("LoadProcessId", "BIGINT", Process.Id);
            new SqlTask(this, Sql_Abort) {
                DisableLogging = true,
                Parameter = new List<QueryParameter>() { cd, em, lpk },
            }.ExecuteNonQuery();
            Process = ReadProcess(Process.Id ?? 0);
            Logging.CurrentLoadProcess = Process;
            return Process;
        }

        public LoadProcess EndProcess(string endMessage = "") {
            QueryParameter cd = new QueryParameter("CurrentDate", "DATETIME", DateTime.Now);
            QueryParameter em = new QueryParameter("EndMessage", "VARCHAR(100)", endMessage);
            QueryParameter lpk = new QueryParameter("LoadProcessId", "BIGINT", Process.Id);
            new SqlTask(this, Sql_End) {
                DisableLogging = true,
                Parameter = new List<QueryParameter>() { cd, em, lpk },
            }.ExecuteNonQuery();
            Process = ReadProcess(Process.Id ?? 0);
            Logging.CurrentLoadProcess = Process;
            return Process;
        }

        public void CreateTable() {
            List<TableColumn> lpColumns = new List<TableColumn>() {
                    new TableColumn("id","BIGINT", allowNulls: false, isPrimaryKey: true, isIdentity:true),
                    new TableColumn("start_date","DATETIME", allowNulls: false),
                    new TableColumn("end_date","DATETIME", allowNulls: true),
                    new TableColumn("source","VARCHAR(20)", allowNulls: true),
                    new TableColumn("source_id","BIGINT", allowNulls: true),
                    new TableColumn("process_name","VARCHAR(100)", allowNulls: false) { DefaultValue = "'N/A'" },
                    new TableColumn("start_message","VARCHAR(2000)", allowNulls: true)  ,
                    new TableColumn("is_running","SMALLINT", allowNulls: false) { DefaultValue = "1" },
                    new TableColumn("end_message","VARCHAR(2000)", allowNulls: true)  ,
                    new TableColumn("was_successful","SMALLINT", allowNulls: false) { DefaultValue = "0" },
                    new TableColumn("abort_message","VARCHAR(2000)", allowNulls: true) ,
                    new TableColumn("was_aborted","SMALLINT", allowNulls: false) { DefaultValue = "0" }
                };
            CreateTableTask LoadProcessTable = new CreateTableTask(TableName, lpColumns) { DisableLogging = true };
            LoadProcessTable.CopyLogTaskProperties(this);
            LoadProcessTable.ConnectionManager = this.ConnectionManager;
            LoadProcessTable.DisableLogging = true;
            LoadProcessTable.CreateIfNotExists();
            Logging.LoadProcessTable = TableName;
        }


        List<LoadProcess> ReadProcessInternal(long processId = 0, ReadOptions readOption = ReadOptions.ReadSingleProcess) {

            var allLoadProcesses = new List<LoadProcess>();
            var loadProcess = new LoadProcess();
            var sql = new SqlTask(this, Sql_Read(processId, readOption)) {
                DisableLogging = true,
                Actions = new List<Action<object>>() {
                col => loadProcess.Id = Convert.ToInt64(col),
                col => loadProcess.StartDate = (DateTime)col,
                col => loadProcess.EndDate = (DateTime?)col,
                col => loadProcess.Source = (string)col,
                col => { if (col == null) loadProcess.SourceId = null; else loadProcess.SourceId =  Convert.ToInt64(col); },
                col => loadProcess.ProcessName = (string)col,
                col => loadProcess.StartMessage = (string)col,
                col => loadProcess.IsRunning = Convert.ToInt16(col) > 0 ? true : false,
                col => loadProcess.EndMessage = (string)col,
                col => loadProcess.WasSuccessful = Convert.ToInt16(col) > 0 ? true : false,
                col => loadProcess.AbortMessage = (string)col,
                col => loadProcess.WasAborted= Convert.ToInt16(col) > 0 ? true : false,
                }
            };
            sql.BeforeRowReadAction = () => loadProcess = new LoadProcess();
            sql.AfterRowReadAction = () => allLoadProcesses.Add(loadProcess);
            sql.ExecuteReader();
            return allLoadProcesses;
        }

        public LoadProcess ReadProcess(long processId) {
            this.Process = ReadProcessInternal(processId).FirstOrDefault();
            return this.Process;
        }

        public string TableName {
            get {
                if (string.IsNullOrWhiteSpace(_tableName))
                    return Logging.LoadProcessTable;
                else
                    return _tableName;
            }
            set {
                _tableName = value;
            }
        }
        public string _tableName;

        public LoadProcess Process { get; set; } = new LoadProcess();

        string PP => this.DbConnectionManager?.PP;

        string Sql_Start => $@"
 INSERT INTO { TN.QuotatedFullName } 
( {QB}start_date{QE}, {QB}process_name{QE}, {QB}start_message{QE}, {QB}source{QE}, {QB}source_id{QE}, {QB}is_running{QE})
 VALUES ({PP}CurrentDate,{PP}ProcessName, {PP}StartMessage,{PP}Source, {PP}SourceId, 1 )
{LastIdSql}";

        string Sql_Abort => $@"
 UPDATE { TN.QuotatedFullName } 
  SET {QB}end_date{QE} = {PP}CurrentDate
  , {QB}is_running{QE} = 0
  , {QB}was_successful{QE} = 0
  , {QB}was_aborted{QE} = 1
  , {QB}abort_message{QE} = {PP}AbortMessage
  WHERE {QB}id{QE} = {PP}LoadProcessId
";

        string Sql_End => $@"
 UPDATE { TN.QuotatedFullName } 
  SET {QB}end_date{QE} = {PP}CurrentDate
  , {QB}is_running{QE} = 0
  , {QB}was_successful{QE} = 1
  , {QB}was_aborted{QE} = 0
  , {QB}end_message{QE} = {PP}EndMessage
  WHERE {QB}id{QE} = {PP}LoadProcessId
";

        string Sql_Read(long processId, ReadOptions readOption) {
            string sql = $@"
SELECT {Top1Sql(readOption)} {QB}id{QE}, {QB}start_date{QE}, {QB}end_date{QE}, {QB}source{QE}, {QB}source_id{QE}, {QB}process_name{QE}, {QB}start_message{QE}, {QB}is_running{QE}, {QB}end_message{QE}, {QB}was_successful{QE}, {QB}abort_message{QE}, {QB}was_aborted{QE}
FROM {TN.QuotatedFullName} ";
            if (readOption == ReadOptions.ReadSingleProcess)
                sql += $@"WHERE {QB}id{QE} = {processId}";
            else if (readOption == ReadOptions.ReadLastFinishedProcess)
                sql += $@"WHERE {QB}was_successful{QE} = 1 || {QB}was_aborted{QE} = 1
ORDER BY {QB}end_date{QE} DESC, {QB}id{QE} DESC";
            else if (readOption == ReadOptions.ReadLastSuccessful)
                sql += $@"WHERE {QB}was_successful{QE} = 1
ORDER BY {QB}end_date{QE} DESC, {QB}id{QE} DESC";
            else if (readOption == ReadOptions.ReadLastAborted)
                sql += $@"WHERE {QB}was_aborted{QE} = 1
ORDER BY {QB}end_date{QE} DESC, {QB}id{QE} DESC";
            sql += Environment.NewLine + Limit1Sql(readOption);
            return sql;

        }

        private string Top1Sql(ReadOptions readOption) {
            if (readOption == ReadOptions.ReadAllProcesses)
                return string.Empty;
            else {
                if (ConnectionType == ConnectionManagerType.SqlServer)
                    return "TOP 1";
                else
                    return string.Empty;
            }

        }

        private string Limit1Sql(ReadOptions readOption) {
            if (readOption == ReadOptions.ReadAllProcesses)
                return string.Empty;
            else {
                if (ConnectionType == ConnectionManagerType.Postgres)
                    return "LIMIT 1";
                else
                    return string.Empty;
            }
        }

        ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);

        string LastIdSql {
            get {
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

        public LoadProcessTask() {

        }
        public LoadProcessTask(string processName) : this() {
            this.Process.ProcessName = processName;
        }

        public LoadProcessTask(string processName, string source) : this(processName) {
            this.Process.Source = source;
        }

        public LoadProcessTask(string processName, long sourceId) : this(processName) {
            this.Process.SourceId = sourceId;
        }

        public static LoadProcess Start(string processName) => new LoadProcessTask(processName).StartProcess();
        public static LoadProcess Start(string processName, string startMessage) => new LoadProcessTask(processName).StartProcess(startMessage);
        public static LoadProcess Start(string processName, string startMessage, string source) => new LoadProcessTask(processName, source).StartProcess(startMessage);
        public static LoadProcess Start(string processName, string startMessage, long sourceId) => new LoadProcessTask(processName, sourceId).StartProcess(startMessage);
        public static LoadProcess Start(IConnectionManager connectionManager, string processName)
            => new LoadProcessTask(processName) { ConnectionManager = connectionManager }.StartProcess();
        public static LoadProcess Start(IConnectionManager connectionManager, string processName, string startMessage)
            => new LoadProcessTask(processName) { ConnectionManager = connectionManager }.StartProcess(startMessage);
        public static LoadProcess Start(IConnectionManager connectionManager, string processName, string startMessage, string source)
            => new LoadProcessTask(processName, source) { ConnectionManager = connectionManager }.StartProcess(startMessage);
        public static LoadProcess Start(IConnectionManager connectionManager, string processName, string startMessage, long sourceId)
            => new LoadProcessTask(processName, sourceId) { ConnectionManager = connectionManager }.StartProcess(startMessage);

        public static LoadProcess Abort(LoadProcess process) => new LoadProcessTask() { Process = process }.AbortProcess();
        public static LoadProcess Abort(LoadProcess process, string abortMessage) => new LoadProcessTask() { Process = process }.AbortProcess(abortMessage);
        public static LoadProcess Abort(IConnectionManager connectionManager, LoadProcess process)
            => new LoadProcessTask() { ConnectionManager = connectionManager, Process = process }.AbortProcess();
        public static LoadProcess Abort(IConnectionManager connectionManager, LoadProcess process, string abortMessage)
            => new LoadProcessTask() { ConnectionManager = connectionManager, Process = process }.AbortProcess(abortMessage);
        public static LoadProcess End(LoadProcess process) => new LoadProcessTask() { Process = process }.EndProcess();
        public static LoadProcess End(LoadProcess process, string endMessage) => new LoadProcessTask() { Process = process }.EndProcess(endMessage);
        public static LoadProcess End(IConnectionManager connectionManager, LoadProcess process)
            => new LoadProcessTask() { ConnectionManager = connectionManager, Process = process }.EndProcess();
        public static LoadProcess End(IConnectionManager connectionManager, LoadProcess process, string endMessage)
            => new LoadProcessTask() { ConnectionManager = connectionManager, Process = process }.EndProcess(endMessage);

        public static void CreateTable(string tableName = Logging.DEFAULTLOADPROCESSTABLENAME)
         => new LoadProcessTask { TableName = tableName }.CreateTable();
        public static void CreateTable(IConnectionManager connectionManager, string tableName = Logging.DEFAULTLOADPROCESSTABLENAME)
            => new LoadProcessTask() { ConnectionManager = connectionManager, TableName = tableName }.CreateTable();

        public static List<LoadProcess> ReadAll()
           => new LoadProcessTask().ReadProcessInternal(readOption: ReadOptions.ReadAllProcesses);
        public static List<LoadProcess> ReadAll(IConnectionManager connectionManager)
            => new LoadProcessTask() { ConnectionManager = connectionManager }.ReadProcessInternal(readOption: ReadOptions.ReadAllProcesses);

        public static LoadProcess Read(long processId)
       => new LoadProcessTask().ReadProcessInternal(processId).FirstOrDefault();
        public static LoadProcess Read(IConnectionManager connectionManager, long processId)
        => new LoadProcessTask() { ConnectionManager = connectionManager }.ReadProcessInternal(processId).FirstOrDefault();

        public static LoadProcess ReadLastAborted()
         => new LoadProcessTask().ReadProcessInternal(readOption: ReadOptions.ReadLastAborted).FirstOrDefault();
        public static LoadProcess ReadLastSuccessful()
          => new LoadProcessTask().ReadProcessInternal(readOption: ReadOptions.ReadLastSuccessful).FirstOrDefault();
        public static LoadProcess ReadLastFinished()
          => new LoadProcessTask().ReadProcessInternal(readOption: ReadOptions.ReadLastFinishedProcess).FirstOrDefault();
        public static LoadProcess ReadLastAborted(IConnectionManager connectionManager)
          => new LoadProcessTask() { ConnectionManager = connectionManager }.ReadProcessInternal(readOption: ReadOptions.ReadLastAborted).FirstOrDefault();
        public static LoadProcess ReadLastSuccessful(IConnectionManager connectionManager)
          => new LoadProcessTask() { ConnectionManager = connectionManager }.ReadProcessInternal(readOption: ReadOptions.ReadLastSuccessful).FirstOrDefault();
        public static LoadProcess ReadLastFinished(IConnectionManager connectionManager)
          => new LoadProcessTask() { ConnectionManager = connectionManager }.ReadProcessInternal(readOption: ReadOptions.ReadLastFinishedProcess).FirstOrDefault();

    }

    internal enum ReadOptions
    {
        ReadSingleProcess,
        ReadAllProcesses,
        ReadLastFinishedProcess,
        ReadLastSuccessful,
        ReadLastAborted
    }
}
