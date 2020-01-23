using Newtonsoft.Json;
using System;
using System.IO;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A Json destination defines a json file where data from the flow is inserted. Inserts are done in batches.
    /// </summary>
    /// <see cref="JsonDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    /// <example>
    /// <code>
    /// JsonDestination&lt;MyRow&gt; dest = new JsonDestination&lt;MyRow&gt;("/path/to/file.json");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class JsonDestination<TInput> : DataFlowBatchDestination<TInput>, ITask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName => $"Write Json into file {FileName ?? ""}";

        public string FileName { get; set; }
        public bool HasFileName => !String.IsNullOrWhiteSpace(FileName);
        public JsonSerializer JsonSerializer { get; set; }

        internal const int DEFAULT_BATCH_SIZE = 1000;
        StreamWriter StreamWriter { get; set; }
        JsonTextWriter JsonTextWriter { get; set; }


        public JsonDestination()
        {
            BatchSize = DEFAULT_BATCH_SIZE;
        }

        public JsonDestination(string fileName) : this()
        {
            FileName = fileName;
        }

        protected override void InitObjects(int batchSize)
        {
            base.InitObjects(batchSize);
            JsonSerializer = new JsonSerializer()
            {
                NullValueHandling = NullValueHandling.Ignore,
                Formatting = Formatting.Indented
            };
        }

        protected void InitJsonWriter()
        {
            StreamWriter = new StreamWriter(FileName);
            JsonTextWriter = new JsonTextWriter(StreamWriter);
            JsonTextWriter.Formatting = JsonSerializer.Formatting;
            if (ErrorHandler.HasErrorBuffer)
                JsonSerializer.Error += (sender, args) =>
                {
                    ErrorHandler.Send(args.ErrorContext.Error, ErrorHandler.ConvertErrorData(args.CurrentObject));
                    args.ErrorContext.Handled = true;
                };
            JsonTextWriter.WriteStartArray();
            this.CloseStreamsAction = CloseStreams;

        }

        protected override void WriteBatch(ref TInput[] data)
        {
            if (JsonTextWriter == null) InitJsonWriter();
            base.WriteBatch(ref data);
            foreach (var record in data)
            {
                if (record == null) continue;
                JsonSerializer.Serialize(JsonTextWriter, record);
            }
            LogProgressBatch(data.Length);
        }

        public void CloseStreams()
        {
            JsonTextWriter.WriteEndArray();
            JsonTextWriter?.Flush();
            StreamWriter?.Flush();
            JsonTextWriter?.Close();
            StreamWriter?.Close();
        }
    }

    /// <summary>
    /// A Json destination defines a json file where data from the flow is inserted. Inserts are done in batches (using Bulk insert).
    /// The JsonDestination access a string array as input type. If you need other data types, use the generic CSVDestination instead.
    /// </summary>
    /// <see cref="JsonDestination{TInput}"/>
    /// <example>
    /// <code>
    /// //Non generic JsonDestination works with string[] as input
    /// //use JsonDestination&lt;TInput&gt; for generic usage!
    /// JsonDestination dest = new JsonDestination("/path/to/file.json");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class JsonDestination : JsonDestination<string[]>
    {
        public JsonDestination() : base() { }

        public JsonDestination(string fileName) : base(fileName) { }

    }

}
