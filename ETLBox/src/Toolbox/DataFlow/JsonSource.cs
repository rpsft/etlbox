using Newtonsoft.Json;
using System;
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
    public class JsonSource<TOutput> : DataFlowSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName => $"Read Json  from {Uri ?? ""}";

        /* Public properties */
        /// <summary>
        /// The Url of the webservice (e.g. https://test.com/foo) or the file name (relative or absolute)
        /// </summary>
        public string Uri { get; set; }
        /// <summary>
        /// Specifies the resourc type. By default requests are made with HttpClient.
        /// Specify ResourceType.File if you want to read from a json file.
        /// </summary>
        public ResourceType ResourceType { get; set; }
        /// <summary>
        /// The Newtonsoft.Json.JsonSerializer used to deserialize the json into the used data type.
        /// </summary>
        public JsonSerializer JsonSerializer { get; set; }

        /* Private stuff */
        JsonTextReader JsonTextReader { get; set; }
        StreamReader StreamReader { get; set; }
        HttpClient HttpClient { get; set; }

        public JsonSource() : base()
        {
            TypeInfo = new TypeInfo(typeof(TOutput));
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


        public override void Execute()
        {
            NLogStart();
            Open();
            try
            {
                ReadAll();
                Buffer.Complete();
            }
            finally
            {
                Close();
            }
            NLogFinish();
        }

        private void Open()
        {
            if (ResourceType == ResourceType.File)
            {
                StreamReader = new StreamReader(Uri, true);
            }
            else
            {
                HttpClient = new HttpClient();
                StreamReader = new StreamReader(HttpClient.GetStreamAsync(new Uri(Uri)).Result);
            }
            JsonTextReader = new JsonTextReader(StreamReader);
        }


        private void ReadAll()
        {
            JsonTextReader.Read();
            if (JsonTextReader.TokenType != JsonToken.StartArray)
                throw new ETLBoxException("Json needs to contain an array on root level!");

            bool skipRecord = false;
            if (ErrorHandler.HasErrorBuffer)
                JsonSerializer.Error += (sender, args) =>
                {
                    ErrorHandler.Post(args.ErrorContext.Error, args.ErrorContext.Error.Message);
                    args.ErrorContext.Handled = true;
                    skipRecord = true;
                };
            while (JsonTextReader.Read())
            {
                if (JsonTextReader.TokenType == JsonToken.EndArray) continue;
                else
                {
                    TOutput record = JsonSerializer.Deserialize<TOutput>(JsonTextReader);
                    if (skipRecord)
                    {
                        if (JsonTextReader.TokenType == JsonToken.EndObject)
                            skipRecord = false;
                        continue;
                    }
                    Buffer.Post(record);
                    LogProgress(1);
                }
            }
        }

        private void Close()
        {
            JsonTextReader?.Close();
            StreamReader?.Dispose();
            HttpClient?.Dispose();
        }
    }

    /// <summary>
    /// Reads data from a json source. While reading the data from the file, data is also asnychronously posted into the targets.
    /// JsonSource as a nongeneric type always return a string array as output. If you need typed output, use
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
    public class JsonSource : JsonSource<string[]>
    {
        public JsonSource() : base() { }
        public JsonSource(string uri) : base(uri) { }
        public JsonSource(string uri, ResourceType resourceType) : base(uri, resourceType) { }
    }
}
