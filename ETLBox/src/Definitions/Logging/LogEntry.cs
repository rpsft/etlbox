using System.Diagnostics;
using Newtonsoft.Json;

namespace ALE.ETLBox.Logging
{
    [DebuggerDisplay("#{Id} {TaskType} - {TaskAction} {LogDate}")]
    public class LogEntry
    {
        public DateTime LogDate { get; set; }
        public DateTime? EndDate { get; set; }
        public string Level { get; set; }
        public string Message { get; set; }
        public string TaskType { get; set; }
        public string TaskAction { get; set; }
        public string TaskHash { get; set; }
        public string Stage { get; set; }
        public string Source { get; set; }
        public long? LoadProcessId { get; set; }
    }

    [PublicAPI]
    [DebuggerDisplay("#{Id} {TaskType} - {TaskAction} {LogDate}")]
    public class LogHierarchyEntry : LogEntry
    {
        public List<LogHierarchyEntry> Children { get; set; }

        [JsonIgnore]
        public LogHierarchyEntry Parent { get; set; }

        public LogHierarchyEntry()
        {
            Children = new List<LogHierarchyEntry>();
        }

        public LogHierarchyEntry(LogEntry entry)
            : this()
        {
            LogDate = entry.LogDate;
            EndDate = entry.EndDate;
            Level = entry.Level;
            Message = entry.Message;
            TaskType = entry.TaskType;
            TaskAction = entry.TaskAction;
            TaskHash = entry.TaskHash;
            Stage = entry.Stage;
            Source = entry.Source;
            LoadProcessId = entry.LoadProcessId;
        }
    }
}
