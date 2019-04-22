using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Reads data from a csv source. While reading the data from the file, data is also asnychronously posted into the targets.
    /// Data is read a as string from the source and dynamically converted into the corresponding data format.
    /// </summary>
    /// <example>
    /// <code>
    /// CSVSource&lt;CSVData&gt; source = new CSVSource&lt;CSVData&gt;("Demo.csv");
    /// source.Configuration.Delimiter = ";";
    /// </code>
    /// </example>
    public class CSVSource<TOutput> : GenericTask, ITask, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_CSVSOURCE";
        public override string TaskName => $"Dataflow: Read CSV Source data from file: {FileName}";
        public override void Execute() => ExecuteAsync();

        /* Public properties */
        public Configuration Configuration { get; set; }
        public int SkipRows { get; set; } = 0;
        public string FileName { get; set; }
        public string[] FieldHeaders { get; private set; }

        public bool IsHeaderRead => FieldHeaders != null;
        public ISourceBlock<TOutput> SourceBlock => this.Buffer;

        /* Private stuff */
        CsvReader CsvReader { get; set; }
        StreamReader StreamReader { get; set; }
        BufferBlock<TOutput> Buffer { get; set; }
        NLog.Logger NLogger { get; set; }
        TypeInfo TypeInfo { get; set; }

        public CSVSource()
        {
            NLogger = NLog.LogManager.GetLogger("ETL");
            Buffer = new BufferBlock<TOutput>();
            TypeInfo = new TypeInfo(typeof(TOutput));
            Configuration = new Configuration(CultureInfo.InvariantCulture);
        }

        public CSVSource(string fileName) : this()
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
                throw new ETLBoxException("Error during reading data from csv file - see inner exception for details.", e);
            }
            finally
            {
                Close();
            }
            NLogFinish();
        }

        private void Open()
        {
            StreamReader = new StreamReader(FileName, Configuration.Encoding ?? Encoding.UTF8);
            SkipFirstRows();
            CsvReader = new CsvReader(StreamReader, Configuration);
        }

        private void SkipFirstRows()
        {
            for (int i = 0; i < SkipRows; i++)
                StreamReader.ReadLine();
        }


        private async Task ReadAll()
        {
            CsvReader.Read();
            CsvReader.ReadHeader();
            FieldHeaders = CsvReader.Context.HeaderRecord;
            while (CsvReader.Read())
            {
                await ReadLineAndSendIntoBuffer();
            }
        }

        private async Task ReadLineAndSendIntoBuffer()
        {
            if (TypeInfo.IsArray)
            {
                string[] line = CsvReader.Context.Record;
                await Buffer.SendAsync((TOutput)(object)line);
            }
            else
            {
                TOutput bufferObject = CsvReader.GetRecord<TOutput>();
                await Buffer.SendAsync(bufferObject);
            }
        }

        private void Close()
        {
            CsvReader?.Dispose();
            CsvReader = null;
            StreamReader?.Dispose();
            StreamReader = null;
        }

        public void LinkTo(IDataFlowLinkTarget<TOutput> target)
        {
            Buffer.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        public void LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
        {
            Buffer.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true }, predicate);
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void NLogFinish()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }
    }

    /// <summary>
    /// Reads data from a csv source. While reading the data from the file, data is also asnychronously posted into the targets.
    /// CSVSource as a nongeneric type always return a string array as output. If you need typed output, use
    /// the CSVSource&lt;TOutput&gt; object instead.
    /// </summary>
    /// <see cref="CSVSource{TOutput}"/>
    /// <example>
    /// <code>
    /// CSVSource source = new CSVSource("demodata.csv");
    /// source.LinkTo(dest); //Link to transformation or destination
    /// source.Execute(); //Start the dataflow
    /// </code>
    /// </example>
    public class CSVSource : CSVSource<string[]>
    {
        public CSVSource() : base() { }
        public CSVSource(string fileName) : base(fileName) { }
    }
}
