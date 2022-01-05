using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace ALE.ETLBox.Logging
{
    [DebuggerDisplay("#{Id} {TaskType} - {TaskAction} {LogDate}")]
    public class LogEntry
    {
        public long Id { get; set; }
        public DateTime LogDate { get; set; }
        public DateTime StartDate => LogDate;
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

    [DebuggerDisplay("#{ID} {TaskType} - {TaskAction} {LogDate}")]
    public class LogHierarchyEntry : LogEntry
    {
        public List<LogHierarchyEntry> Children { get; set; }
        [JsonIgnore]
        public LogHierarchyEntry Parent { get; set; }
        public LogHierarchyEntry()
        {
            Children = new List<LogHierarchyEntry>();
        }
        public LogHierarchyEntry(LogEntry entry) : this()
        {
            this.Id = entry.Id;
            this.LogDate = entry.LogDate;
            this.EndDate = entry.EndDate;
            this.Level = entry.Level;
            this.Message = entry.Message;
            this.TaskType = entry.TaskType;
            this.TaskAction = entry.TaskAction;
            this.TaskHash = entry.TaskHash;
            this.Stage = entry.Stage;
            this.Source = entry.Source;
            this.LoadProcessId = entry.LoadProcessId;
        }
    }
}
