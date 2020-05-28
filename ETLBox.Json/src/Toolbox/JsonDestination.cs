using ETLBox.DataFlow;
using Newtonsoft.Json;
using System.Dynamic;

namespace ETLBox.Json
{
    /// <summary>
    /// A Json destination defines a json file where data from the flow is inserted. 
    /// </summary>
    /// <see cref="JsonDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    /// <example>
    /// <code>
    /// JsonDestination&lt;MyRow&gt; dest = new JsonDestination&lt;MyRow&gt;("/path/to/file.json");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class JsonDestination<TInput> : DataFlowStreamDestination<TInput>, ITask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName => $"Write Json into file {Uri ?? ""}";

        public JsonSerializer JsonSerializer { get; set; }
        JsonTextWriter JsonTextWriter { get; set; }

        public JsonDestination() : base()
        {
            JsonSerializer = new JsonSerializer()
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.Indented
            };
            InitTargetAction();
        }

        public JsonDestination(string uri) : this()
        {
            Uri = uri;
        }

        public JsonDestination(string uri, ResourceType resourceType) : this(uri)
        {
            ResourceType = resourceType;
        }

        protected override void InitStream()
        {
            JsonTextWriter = new JsonTextWriter(StreamWriter);
            JsonTextWriter.Formatting = JsonSerializer.Formatting;
            if (ErrorHandler.HasErrorBuffer)
                JsonSerializer.Error += (sender, args) =>
                {
                    ErrorHandler.Send(args.ErrorContext.Error, ErrorHandler.ConvertErrorData(args.CurrentObject));
                    args.ErrorContext.Handled = true;
                };
            JsonTextWriter.WriteStartArray();
        }

        protected override void WriteIntoStream(TInput data)
        {
            if (data != null)
            {
                JsonSerializer.Serialize(JsonTextWriter, data);
                LogProgress();
            }
        }

        protected override void CloseStream()
        {
            JsonTextWriter?.WriteEndArray();
            JsonTextWriter?.Flush();
            JsonTextWriter?.Close();
        }
    }

    /// <summary>
    /// A Json destination defines a json file where data from the flow is inserted. 
    /// The JsonDestination uses a dynamic object as input type. If you need other data types, use the generic CsvDestination instead.
    /// </summary>
    /// <see cref="JsonDestination{TInput}"/>
    /// <example>
    /// <code>
    /// //Non generic JsonDestination works with dynamic object as input
    /// //use JsonDestination&lt;TInput&gt; for generic usage!
    /// JsonDestination dest = new JsonDestination("/path/to/file.json");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class JsonDestination : JsonDestination<ExpandoObject>
    {
        public JsonDestination() : base() { }

        public JsonDestination(string fileName) : base(fileName) { }

    }

}
