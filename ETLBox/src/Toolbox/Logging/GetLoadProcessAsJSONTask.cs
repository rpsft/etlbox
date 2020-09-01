using ETLBox.Connection;
using ETLBox.ControlFlow;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System.Collections.Generic;

namespace ETLBox.Logging
{
    /// <summary>
    /// Returns the content of the LoadProcess table as JSON.
    /// The table name is read from `ControlFlow.LoadProcessTable`. The default table name is etlbox_log.
    /// </summary>
    public class GetLoadProcessAsJSONTask : ControlFlowTask
    {
        /* ITask Interface */
        public override string TaskName => $"Get load process list as JSON";

        public void Execute()
        {
            var read = new ReadLoadProcessTableTask()
            {
                ReadOption = ReadOptions.ReadAllProcesses,
                ConnectionManager = this.ConnectionManager

            };
            read.Execute();
            List<LoadProcess> logEntries = read.AllLoadProcesses;
            JSON = JsonConvert.SerializeObject(logEntries, new JsonSerializerSettings
            {
                Formatting = Formatting.Indented,
                ContractResolver = new CamelCasePropertyNamesContractResolver(),
                NullValueHandling = NullValueHandling.Ignore
            });

        }

        public string JSON { get; private set; }

        public GetLoadProcessAsJSONTask Create()
        {
            this.Execute();
            return this;
        }

        public GetLoadProcessAsJSONTask()
        {

        }

        public static string GetJSON() => new GetLoadProcessAsJSONTask().Create().JSON;
        public static string GetJSON(IConnectionManager connectionManager)
            => new GetLoadProcessAsJSONTask() { ConnectionManager = connectionManager }.Create().JSON;

    }
}
