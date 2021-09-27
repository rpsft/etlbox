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
    /// Used this task for custom log messages.
    /// </summary>
    public sealed class LogTask : ControlFlowTask
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = $"Manages log entries";

        /* Public properties */

        public string TableName {
            get {
                if (string.IsNullOrWhiteSpace(_tableName))
                    return Logging.LogTable;
                else
                    return _tableName;
            }
            set {
                _tableName = value;
            }
        }
        public string _tableName;

        public string Message { get; set; }

        public object[] Args { get; set; }

        public LogTask() {
        }

        public LogTask(string message) : this() {
            Message = message;
        }

        public LogTask(string message, params object[] args) : this(message) {
            Args = args;
        }

        public void CreateLogTable() {
            List<TableColumn> columns = new List<TableColumn>() {
                new TableColumn("id","BIGINT", allowNulls: false, isPrimaryKey: true, isIdentity:true),
                new TableColumn("log_date","DATETIME", allowNulls: false),
                new TableColumn("level","VARCHAR(10)", allowNulls: true),
                new TableColumn("stage","VARCHAR(20)", allowNulls: true),
                new TableColumn("message","VARCHAR(4000)", allowNulls: true),
                new TableColumn("task_name","VARCHAR(1000)", allowNulls: true),
                new TableColumn("task_type","VARCHAR(200)", allowNulls: true),
                new TableColumn("action","VARCHAR(5)", allowNulls: true),
                new TableColumn("task_hash","CHAR(40)", allowNulls: true),
                new TableColumn("source","VARCHAR(20)", allowNulls: true),
                new TableColumn("load_process_id","BIGINT", allowNulls: true)
            };
            var logTable = new CreateTableTask(TableName, columns);

            logTable.CopyLogTaskProperties(this);
            logTable.ConnectionManager = this.ConnectionManager;
            logTable.DisableLogging = true;
            logTable.CreateIfNotExists();
            Logging.LogTable = TableName;
        }

        ObjectNameDescriptor TN => new ObjectNameDescriptor(TableName, QB, QE);

        string Sql_Read(long? loadProcessId) => $@"
SELECT {QB}id{QE}, {QB}log_date{QE}, {QB}level{QE}, {QB}message{QE}, {QB}task_name{QE}, {QB}task_type{QE}, {QB}action{QE}, {QB}task_hash{QE}, {QB}stage{QE}, {QB}source{QE}, {QB}load_process_id{QE}
FROM { TN.QuotatedFullName}" +
            (loadProcessId != null ? $@" WHERE {QB}LoadProcessKey{QE} = {loadProcessId}"
            : "")
            + $@" ORDER BY {QB}id{QE}";

        public List<LogEntry> ReadLogTable(long? loadProcessId = null) {
            var logEntries = new List<LogEntry>();
            LogEntry current = new LogEntry();
            new SqlTask(this, Sql_Read(loadProcessId)) {
                DisableLogging = true,
                ConnectionManager = this.ConnectionManager,
                BeforeRowReadAction = () => current = new LogEntry(),
                AfterRowReadAction = () => logEntries.Add(current),
                Actions = new List<Action<object>>() {
                    col => current.Id = Convert.ToInt64(col),
                    col => current.LogDate = (DateTime)col,
                    col => current.Level = (string)col,
                    col => current.Message = (string)col,
                    col => current.TaskName = (string)col,
                    col => current.TaskType = (string)col,
                    col => current.Action = (string)col,
                    col => current.TaskHash = (string)col,
                    col => current.Stage = (string)col,
                    col => current.Source = (string)col,
                    col => current.LoadProcessId = Convert.ToInt64(col),
                }
            }.ExecuteReader();
            return logEntries;
        }

        public static LogHierarchyEntry ConvertToHierachy(List<LogEntry> logEntries) {
            CalculateEndDate(logEntries);
            return CreateHierarchyStructure(logEntries);
        }

        private static void CalculateEndDate(List<LogEntry> logEntries) {
            foreach (var startEntry in logEntries.Where(entry => entry.Action == "START")) {
                var endEntry = logEntries.Where(entry => entry.Action == "END" && entry.TaskHash == startEntry.TaskHash && entry.Id > startEntry.Id).FirstOrDefault();
                startEntry.EndDate = endEntry.LogDate;
            }
        }

        public static string[] ContainerTypeNames { get; set; } = new[] { "LogSection" };

        private static LogHierarchyEntry CreateHierarchyStructure(List<LogEntry> entries) {
            LogHierarchyEntry root = new LogHierarchyEntry(new LogEntry() { TaskType = "ROOT" });
            var currentParent = root;
            var currentList = root.Children;
            foreach (LogEntry entry in entries) {
                if (ContainerTypeNames.Contains(entry.TaskType) && entry.Action == "START") {
                    var newEntry = new LogHierarchyEntry(entry) { Parent = currentParent };
                    currentList.Add(newEntry);
                    currentParent = newEntry;
                    currentList = newEntry.Children;
                } else if (ContainerTypeNames.Contains(entry.TaskType) && entry.Action == "END") {
                    currentParent = currentParent.Parent;
                    currentList = currentParent.Children;
                } else if (entry.Action != "END") {
                    var hierarchyEntry = new LogHierarchyEntry(entry) { Parent = currentParent };
                    currentList.Add(hierarchyEntry);
                }
            }
            return root;
        }

        public void Trace() => LogTrace(Message, Args);
        public void Debug() => LogDebug(Message, Args);
        public void Info() => LogInfo(Message, Args);
        public void Warn() => LogWarn(Message, Args);
        public void Error() => LogError(Message, Args);
        public void Fatal() => LogFatal(Message, Args);
        public static void Trace(string message) => new LogTask(message).Trace();
        public static void Debug(string message) => new LogTask(message).Debug();
        public static void Info(string message) => new LogTask(message).Info();
        public static void Warn(string message) => new LogTask(message).Warn();
        public static void Error(string message) => new LogTask(message).Error();
        public static void Fatal(string message) => new LogTask(message).Fatal();

        public static void Trace(string message, params object[] args) => new LogTask(message, args).Trace();
        public static void Debug(string message, params object[] args) => new LogTask(message, args).Debug();
        public static void Info(string message, params object[] args) => new LogTask(message, args).Info();
        public static void Warn(string message, params object[] args) => new LogTask(message, args).Warn();
        public static void Error(string message, params object[] args) => new LogTask(message, args).Error();
        public static void Fatal(string message, params object[] args) => new LogTask(message, args).Fatal();

        public static void CreateLogTable(string logTableName = Logging.DEFAULTLOGTABLENAME)
            => new LogTask() { TableName = logTableName }.CreateLogTable();
        public static void CreateLogTable(IConnectionManager connectionManager, string logTableName = Logging.DEFAULTLOGTABLENAME)
            => new LogTask() { ConnectionManager = connectionManager, TableName = logTableName }.CreateLogTable();

        public static List<LogEntry> ReadLogTable()
            => new LogTask().ReadLogTable();
        public static List<LogEntry> ReadLogTable(long loadProcessId)
            => new LogTask().ReadLogTable(loadProcessId);
        public static List<LogEntry> ReadLogTable(IConnectionManager connectionManager)
            => new LogTask() { ConnectionManager = connectionManager }.ReadLogTable();
        public static List<LogEntry> ReadLogTable(IConnectionManager connectionManager, long loadProcessId)
            => new LogTask() { ConnectionManager = connectionManager }.ReadLogTable(loadProcessId);
    }
}
