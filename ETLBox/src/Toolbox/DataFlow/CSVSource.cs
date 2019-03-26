using CsvHelper;
using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow {
    /// <summary>
    /// Reads data from a csv source. While reading the data from the file, data is also asnychronously posted into the targets.
    /// Data is read a as string from the source and dynamically converted into the corresponding data format.
    /// </summary>
    /// <example>
    /// <code>
    /// CSVSource&lt;CSVData&gt; source = new CSVSource&lt;CSVData&gt;("Demo.csv") {
    ///    Delimiter = ";",
    ///    SourceCommentRows = 2
    /// }
    /// </code>
    /// </example>
    public class CSVSource<TOutput> : GenericTask, ITask, IDataFlowSource<TOutput> {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_CSVSOURCE";
        public override string TaskName => $"Dataflow: Read CSV Source data from file: {FileName}";
        public override void Execute() => ExecuteAsync();

        /* Public properties */
        public int CSVReaderBufferSize { get; set; } = 2048;
        public Encoding Encoding { get; set; } = Encoding.UTF8;
        public int SourceCommentRows { get; set; } = 0;
        public bool TrimFields { get; set; } = true;
        public string Delimiter { get; set; } = ",";
        public char Esacpe { get; set; } = '"';
        public char Quote { get; set; } = '"';
        public bool LineBreakInQuotedFieldIsBadData { get; set; }
        public bool IgnoreQuotes { get; set; }
        public bool AllowComments { get; set; } = true;
        public char Comment { get; set; } = '/';
        public bool SkipEmptyRecords { get; set; } = true;
        public bool IgnoreBlankLines { get; set; } = true;
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

        public CSVSource() {
            NLogger = NLog.LogManager.GetLogger("ETL");
            Buffer = new BufferBlock<TOutput>();
            TypeInfo = new TypeInfo(typeof(TOutput));
        }

        public CSVSource(string fileName) : this() {
            FileName = fileName;
        }

        public void ExecuteAsync() {
            NLogStart();
            Open();
            try {
                ReadAll().Wait();
                Buffer.Complete();
            } catch (Exception e) {
                throw new ETLBoxException("Error during reading data from csv file - see inner exception for details.", e);
            } finally {
                Close();
            }
            NLogFinish();
        }

        private void Open() {
            StreamReader = new StreamReader(FileName, Encoding.UTF8);
            SkipSourceCommentRows();
            CsvReader = new CsvReader(StreamReader);
            ConfigureCSVReader();
        }
        private void SkipSourceCommentRows() {
            for (int i = 0; i < SourceCommentRows; i++)
                StreamReader.ReadLine();
        }

        private async Task ReadAll() {
            CsvReader.Read();
            CsvReader.ReadHeader();
            FieldHeaders = CsvReader.Context.HeaderRecord;
            while (CsvReader.Read()) {
                string[] line = CsvReader.Context.Record;
                await SendIntoBuffer(line);
            }
        }

        private async Task SendIntoBuffer(string[] line) {
            if (TypeInfo.IsArray) {
                await Buffer.SendAsync((TOutput)(object)line);
            } else {
                TOutput bufferObject = (TOutput)Activator.CreateInstance(typeof(TOutput));
                for (int col=0;col < line.Length;col++) {
                    if (col > TypeInfo.PropertyLength) break;
                    TypeInfo.PropertyInfos[col].SetValue(bufferObject, TypeInfo.CastPropertyValue(TypeInfo.PropertyInfos[col], line[col]));
                }
                await Buffer.SendAsync(bufferObject);
            }


        }

        private void ConfigureCSVReader() {

            CsvReader.Configuration.BufferSize = CSVReaderBufferSize;
            CsvReader.Configuration.Delimiter = Delimiter;
            CsvReader.Configuration.Escape = Esacpe;
            CsvReader.Configuration.Quote = Quote;
            CsvReader.Configuration.IgnoreQuotes = IgnoreQuotes;
            CsvReader.Configuration.AllowComments = AllowComments;
            CsvReader.Configuration.Comment = Comment;
            CsvReader.Configuration.IgnoreBlankLines = IgnoreBlankLines;
            CsvReader.Configuration.IgnoreBlankLines = LineBreakInQuotedFieldIsBadData;
            CsvReader.Configuration.TrimOptions = TrimFields ? CsvHelper.Configuration.TrimOptions.Trim : CsvHelper.Configuration.TrimOptions.None;
            CsvReader.Configuration.Encoding = Encoding;
        }

        private void Close() {
            CsvReader?.Dispose();
            CsvReader = null;
            StreamReader?.Dispose();
            StreamReader = null;
        }

        public void LinkTo(IDataFlowLinkTarget<TOutput> target) {
            Buffer.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        public void LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate) {
            Buffer.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true }, predicate);
            NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void NLogStart() {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void NLogFinish() {
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
    public class CSVSource : CSVSource<string[]> {
        public CSVSource() : base() { }
        public CSVSource(string fileName) : base(fileName) { }
    }
}
