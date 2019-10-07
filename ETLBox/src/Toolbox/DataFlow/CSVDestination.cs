using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A Csv destination defines a csv file where data from the flow is inserted. Inserts are done in batches (using Bulk insert).
    /// </summary>
    /// <see cref="DBDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    /// <example>
    /// <code>
    /// CSVDestination&lt;MyRow&gt; dest = new CSVDestination&lt;MyRow&gt;("/path/to/file.csv");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class CSVDestination<TInput> : DataFlowTask, ITask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName => $"Dataflow: Write Data batchwise into file {FileName}";
        public override void Execute() { throw new Exception("Dataflow destinations can't be started directly"); }

        /* Public properties */
        public string FileName { get; set; }
        public bool HasFileName => !String.IsNullOrWhiteSpace(FileName);
        public Func<TInput[], TInput[]> BeforeBatchWrite { get; set; }
        public Action OnCompletion { get; set; }
        public Configuration Configuration
        {
            get
            {
                return configuration;
            }
            set
            {
                configuration = value;
                CsvWriter = new CsvWriter(StreamWriter, value, leaveOpen: true);

            }
        }

        public ITargetBlock<TInput> TargetBlock => Buffer;

        /* Private stuff */
        int BatchSize { get; set; } = DEFAULT_BATCH_SIZE;
        const int DEFAULT_BATCH_SIZE = 1000;
        private Configuration configuration;

        internal BatchBlock<TInput> Buffer { get; set; }
        internal ActionBlock<TInput[]> TargetAction { get; set; }
        private int ThresholdCount { get; set; } = 1;
        TypeInfo TypeInfo { get; set; }
        StreamWriter StreamWriter { get; set; }
        CsvWriter CsvWriter { get; set; }

        public CSVDestination()
        {
            InitObjects(DEFAULT_BATCH_SIZE);

        }

        public CSVDestination(int batchSize)
        {
            BatchSize = batchSize;
            InitObjects(batchSize);
        }

        public CSVDestination(string fileName)
        {
            FileName = fileName;
            InitObjects(DEFAULT_BATCH_SIZE);
        }

        public CSVDestination(string fileName, int batchSize)
        {
            FileName = fileName;
            InitObjects(batchSize);
        }

        private void InitObjects(int batchSize)
        {
            Buffer = new BatchBlock<TInput>(batchSize);
            TargetAction = new ActionBlock<TInput[]>(d => WriteBatch(d));
            Buffer.LinkTo(TargetAction, new DataflowLinkOptions() { PropagateCompletion = true });
            TypeInfo = new TypeInfo(typeof(TInput));
            StreamWriter = new StreamWriter(FileName);
            Configuration = new Configuration(CultureInfo.InvariantCulture);
        }

        private void WriteBatch(TInput[] data)
        {

            if (ProgressCount == 0) NLogStart();
            if (BeforeBatchWrite != null)
                data = BeforeBatchWrite.Invoke(data);
            if (TypeInfo.IsArray)
                WriteArray(data);
            else
                CsvWriter.WriteRecords(data);

            LogProgress(data.Length);
        }

        private void WriteArray(TInput[] data)
        {
            foreach (var record in data)
            {
                var recordAsArray = record as object[];
                foreach (var field in recordAsArray)
                {
                    CsvWriter.WriteField(field);
                }

                CsvWriter.NextRecord();
            }
        }

        public void CloseStreams()
        {
            CsvWriter?.Flush();
            StreamWriter?.Flush();
            CsvWriter?.Dispose();
            StreamWriter?.Dispose();
        }
        public void Wait()
        {
            TargetAction.Completion.Wait();
            CloseStreams();
            OnCompletion?.Invoke();
            NLogFinish();
        }

        void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void NLogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                NLogger.Info(TaskName + $" processed {ProgressCount} records in total.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void LogProgress(int rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (!DisableLogging && HasLoggingThresholdRows && ProgressCount >= (LoggingThresholdRows * ThresholdCount))
            {
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
                ThresholdCount++;
            }
        }
    }

    /// <summary>
    /// A Csv destination defines a csv file where data from the flow is inserted. Inserts are done in batches (using Bulk insert).
    /// The CSVDestination access a string array as input type. If you need other data types, use the generic CSVDestination instead.
    /// </summary>
    /// <see cref="CSVDestination{TInput}"/>
    /// <example>
    /// <code>
    /// //Non generic CSVDestination works with string[] as input
    /// //use CSVDestination&lt;TInput&gt; for generic usage!
    /// CSVDestination dest = new CSVDestination("/path/to/file.csv");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class CSVDestination : CSVDestination<string[]>
    {
        public CSVDestination() : base() { }

        public CSVDestination(int batchSize) : base(batchSize) { }

        public CSVDestination(string fileName) : base(fileName) { }

        public CSVDestination(string fileName, int batchSize) : base(fileName, batchSize) { }

    }

}
