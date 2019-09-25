using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;

namespace ALE.ETLBox.Logging {
    /// <summary>
    /// Reads data from the etl.Log table.
    /// </summary>
    public class ReadLogTableTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskName => $"Read all log entries for {LoadProcessKey ?? 0 }";
        public override void Execute() {
            LogEntries = new List<LogEntry>();
            LogEntry current = new LogEntry();
            new SqlTask(this, Sql) {
                DisableLogging = true,
                ConnectionManager = this.ConnectionManager,
                DisableExtension = true,
                BeforeRowReadAction = () => current = new LogEntry(),
                AfterRowReadAction = () => LogEntries.Add(current),
                Actions = new List<Action<object>>() {
                    col => current.LogKey = (int)col,
                    col => current.LogDate = (DateTime)col,
                    col => current.Level = (string)col,
                    col => current.Message = (string)col,
                    col => current.TaskType = (string)col,
                    col => current.TaskAction = (string)col,
                    col => current.TaskHash = (string)col,
                    col => current.Stage = (string)col,
                    col => current.Source = (string)col,
                    col => current.LoadProcessKey = (int?)col,
                }
            }.ExecuteReader();
        }

        /* Public properties */
        public int? _loadProcessKey;
        public int? LoadProcessKey {
            get {
                return _loadProcessKey ?? ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey;
            }
            set {
                _loadProcessKey = value;
            }
        }

        public ReadLogTableTask ReadLog() {
            Execute();
            return this;
        }

        public List<LogEntry> LogEntries { get; private set; }

        public string Sql => $@"SELECT LogKey, LogDate, Level, Message, TaskType, TaskAction, TaskHash, Stage, Source, LoadProcessKey
    FROM etl.Log" +
            (LoadProcessKey != null ? $@" WHERE LoadProcessKey = {LoadProcessKey}"
            : "");

        public ReadLogTableTask() {

        }

        public ReadLogTableTask(int? loadProcessKey) : this() {
            this.LoadProcessKey = loadProcessKey;
        }

        public static List<LogEntry> Read() => new ReadLogTableTask().ReadLog().LogEntries;
        public static List<LogEntry> Read(int? loadProcessKey) => new ReadLogTableTask(loadProcessKey).ReadLog().LogEntries;
        public static List<LogEntry> Read(IConnectionManager connectionManager)
            => new ReadLogTableTask() { ConnectionManager = connectionManager }.ReadLog().LogEntries;
        public static List<LogEntry> Read(IConnectionManager connectionManager, int? loadProcessKey)
            => new ReadLogTableTask(loadProcessKey) { ConnectionManager = connectionManager }.ReadLog().LogEntries;

    }
}
