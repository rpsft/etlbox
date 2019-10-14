using Newtonsoft.Json;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Reads data from a json source. While reading the data from the file, data is also asnychronously posted into the targets.
    /// </summary>
    /// <example>
    /// <code>
    /// JsonSource&lt;POCO&gt; source = new JsonSource&lt;POCO&gt;("Demo.json");
    /// </code>
    /// </example>
    public class JsonSource<TOutput> : DataFlowSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName => $"Dataflow: Read Json source data from file {FileName}";
        public void Execute() => ExecuteAsync();

        /* Public properties */
        public string FileName { get; set; }
        public JsonSerializer JsonSerializer { get; set; }

        /* Private stuff */
        JsonTextReader JsonTextReader { get; set; }
        StreamReader StreamReader { get; set; }

        public JsonSource() : base()
        {
            TypeInfo = new TypeInfo(typeof(TOutput));
            JsonSerializer = new JsonSerializer();
        }

        public JsonSource(string fileName) : this()
        {
            FileName = fileName;
        }

        public void ExecuteAsync()
        {
            NLogStart();
            Open();
            try
            {
                ReadAll().Wait();
                Buffer.Complete();
            }
            catch (Exception e)
            {
                throw new ETLBoxException("Error during reading data from json file - see inner exception for details.", e);
            }
            finally
            {
                Close();
            }
            NLogFinish();
        }

        private void Open()
        {
            StreamReader = new StreamReader(FileName, Encoding.UTF8);
            JsonTextReader = new JsonTextReader(StreamReader);
        }


        private async Task ReadAll()
        {
            JsonTextReader.Read();
            if (JsonTextReader.TokenType != JsonToken.StartArray)
                throw new ETLBoxException("Json needs to contain an array on root level!");

            while (JsonTextReader.Read())
            {
                if (JsonTextReader.TokenType == JsonToken.EndArray) continue;
                else
                {
                    TOutput record = JsonSerializer.Deserialize<TOutput>(JsonTextReader);
                    await Buffer.SendAsync(record);
                    LogProgress(1);
                }
            }
        }

        private void Close()
        {
            JsonTextReader?.Close();
            StreamReader?.Dispose();
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
        public JsonSource(string fileName) : base(fileName) { }
    }
}
