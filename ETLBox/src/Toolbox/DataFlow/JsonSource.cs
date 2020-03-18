using Newtonsoft.Json;
using System;
using System.Dynamic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Reads data from a json source. This can be any http resource or a file.
    /// By default, data is pulled via httpclient. Use the ResourceType property to read data from a file.
    /// </summary>
    /// <example>
    /// <code>
    /// JsonSource&lt;POCO&gt; source = new JsonSource&lt;POCO&gt;("https://jsonplaceholder.typicode.com/todos");
    /// </code>
    /// </example>
    public class JsonSource<TOutput> : DataFlowStreamSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName => $"Read Json from Uri: {CurrentRequestUri ?? ""}";

        /* Public properties */
        /// <summary>
        /// The Newtonsoft.Json.JsonSerializer used to deserialize the json into the used data type.
        /// </summary>
        public JsonSerializer JsonSerializer { get; set; }

        /* Private stuff */
        JsonTextReader JsonTextReader { get; set; }

        public JsonSource()
        {
            JsonSerializer = new JsonSerializer();
        }

        public JsonSource(string uri) : this()
        {
            Uri = uri;
        }

        public JsonSource(string uri, ResourceType resourceType) : this(uri)
        {
            ResourceType = resourceType;
        }

        protected override void InitReader()
        {
            JsonTextReader = new JsonTextReader(StreamReader);
        }

        protected override void ReadAll()
        {
            do
            {
            } while (JsonTextReader.Read() && JsonTextReader.TokenType != JsonToken.StartArray);

            bool skipRecord = false;
            if (ErrorHandler.HasErrorBuffer)
                JsonSerializer.Error += (sender, args) =>
                {
                    ErrorHandler.Send(args.ErrorContext.Error, args.ErrorContext.Error.Message);
                    args.ErrorContext.Handled = true;
                    skipRecord = true;
                };
            while (JsonTextReader.Read())
            {
                if (JsonTextReader.TokenType == JsonToken.EndArray || JsonTextReader.TokenType == JsonToken.EndObject) continue;
                else
                {
                    TOutput record = JsonSerializer.Deserialize<TOutput>(JsonTextReader);
                    if (skipRecord)
                    {
                        if (JsonTextReader.TokenType == JsonToken.EndObject)
                            skipRecord = false;
                        continue;
                    }
                    Buffer.SendAsync(record).Wait();
                    LogProgress();
                }
            }
        }

        protected override void CloseReader()
        {
            JsonTextReader?.Close();
        }
    }

    /// <summary>
    /// Reads data from a json source. While reading the data from the file, data is also asnychronously posted into the targets.
    /// JsonSource as a nongeneric type returns a dynamic object as output. If you need typed output, use
    /// the JsonSource&lt;TOutput&gt; object instead.
    /// </summary>
    /// <see cref="JsonSource{TOutput}"/>
    /// <example>
    /// <code>
    /// JsonSource source = new JsonSource("demodata.json");
    /// source.LinkTo(dest); //Link to transformation or destination
    /// source.Execute(); //Start the dataflow
    /// </code>
    /// </example>
    public class JsonSource : JsonSource<ExpandoObject>
    {
        public JsonSource() : base() { }
        public JsonSource(string uri) : base(uri) { }
        public JsonSource(string uri, ResourceType resourceType) : base(uri, resourceType) { }
    }
}
