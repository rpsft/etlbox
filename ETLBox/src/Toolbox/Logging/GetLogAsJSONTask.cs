using ETLBox.Connection;
using ETLBox.ControlFlow;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System.Collections.Generic;
using System.Linq;

namespace ETLBox.Logging
{
    /// <summary>
    /// Returns the content of the etl.Log table as a JSON string.
    /// </summary>
    public class GetLogAsJSONTask : ControlFlowTask
    {
        /* ITask Interface */
        public override string TaskName => $"Get log as JSON for {LoadProcessKey}";

        public void Execute()
        {
            List<LogEntry> logEntries = ReadLogTableTask.Read(this.ConnectionManager, LoadProcessKey);
            CalculateEndDate(logEntries);
            LogHierarchyEntry hierarchy = CreateHierarchyStructure(logEntries);
            JSON = JsonConvert.SerializeObject(hierarchy, new JsonSerializerSettings
            {
                Formatting = Formatting.Indented,
                ContractResolver = new CamelCasePropertyNamesContractResolver(),
                NullValueHandling = NullValueHandling.Ignore
            });
        }

        private void CalculateEndDate(List<LogEntry> logEntries)
        {
            foreach (var startEntry in logEntries.Where(entry => entry.TaskAction == "START"))
            {
                var endEntry = logEntries.Where(entry => entry.TaskAction == "END" && entry.TaskHash == startEntry.TaskHash && entry.Id > startEntry.Id).FirstOrDefault();
                startEntry.EndDate = endEntry.LogDate;
            }
        }

        private LogHierarchyEntry CreateHierarchyStructure(List<LogEntry> entries)
        {
            LogHierarchyEntry root = new LogHierarchyEntry(new LogEntry() { TaskType = "ROOT" });
            var currentParent = root;
            var currentList = root.Children;
            foreach (LogEntry entry in entries)
            {
                if (ContainerTypes.Contains(entry.TaskType.ToLower()) && entry.TaskAction == "START")
                {
                    var newEntry = new LogHierarchyEntry(entry) { Parent = currentParent };
                    currentList.Add(newEntry);
                    currentParent = newEntry;
                    currentList = newEntry.Children;
                }
                else if (ContainerTypes.Contains(entry.TaskType.ToLower()) && entry.TaskAction == "END")
                {
                    currentParent = currentParent.Parent;
                    currentList = currentParent.Children;
                }
                else if (entry.TaskAction == "START" || entry.TaskAction == "LOG")
                {
                    var hierarchyEntry = new LogHierarchyEntry(entry) { Parent = currentParent };
                    currentList.Add(hierarchyEntry);
                }
            }
            return root;
        }

        /* Public properties */
        public List<string> ContainerTypes => new List<string>() { "sequence", "subpackage", "package" };

        public long? _loadProcessKey;
        public long? LoadProcessKey
        {
            get
            {
                return _loadProcessKey ?? Logging.CurrentLoadProcess?.Id;
            }
            set
            {
                _loadProcessKey = value;
            }
        }

        public List<LogHierarchyEntry> LogEntryHierarchy { get; set; }

        public string JSON { get; private set; }

        public GetLogAsJSONTask Create()
        {
            this.Execute();
            return this;
        }

        public GetLogAsJSONTask()
        {

        }

        public GetLogAsJSONTask(long? loadProcessKey) : this()
        {
            this.LoadProcessKey = loadProcessKey;
        }

        public static string GetJSON() => new GetLogAsJSONTask().Create().JSON;
        public static string GetJSON(long? loadProcessKey) => new GetLogAsJSONTask(loadProcessKey).Create().JSON;
        public static string GetJSON(IConnectionManager connectionManager)
            => new GetLogAsJSONTask() { ConnectionManager = connectionManager }.Create().JSON;
        public static string GetJSON(IConnectionManager connectionManager, int? loadProcessKey)
            => new GetLogAsJSONTask(loadProcessKey) { ConnectionManager = connectionManager }.Create().JSON;

    }
}
