using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Dynamic;
using System.Globalization;
using System.IO;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A Csv destination defines a csv file where data from the flow is inserted. Inserts are done in batches (using Bulk insert).
    /// </summary>
    /// <see cref="DbDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    /// <example>
    /// <code>
    /// CSVDestination&lt;MyRow&gt; dest = new CSVDestination&lt;MyRow&gt;("/path/to/file.csv");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class CsvDestination<TInput> : DataFlowBatchDestination<TInput>, ITask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName => $"Write Csv data into file {FileName ?? ""}";

        public string FileName { get; set; }
        public bool HasFileName => !String.IsNullOrWhiteSpace(FileName);
        public Configuration Configuration { get; set; }

        internal const int DEFAULT_BATCH_SIZE = 1000;
        StreamWriter StreamWriter { get; set; }
        CsvWriter CsvWriter { get; set; }

        public CsvDestination()
        {
            BatchSize = DEFAULT_BATCH_SIZE;
        }

        public CsvDestination(string fileName) : this()
        {
            FileName = fileName;
        }

        protected override void InitObjects(int batchSize)
        {
            base.InitObjects(batchSize);
            Configuration = new Configuration(CultureInfo.InvariantCulture);
            Configuration.TypeConverterOptionsCache.GetOptions<DateTime>().Formats = new[] { "yyyy-MM-dd HH:mm:ss.fff" };

        }

        protected void InitCsvWriter()
        {
            StreamWriter = new StreamWriter(FileName);
            CsvWriter = new CsvWriter(StreamWriter, Configuration, leaveOpen: true);
            this.CloseStreamsAction = CloseStreams;
        }

        protected override void WriteBatch(ref TInput[] data)
        {
            if (CsvWriter == null)
            {
                InitCsvWriter();
                WriteHeaderIfRequired();
            }
            base.WriteBatch(ref data);

            if (TypeInfo.IsArray)
                WriteArray(ref data);
            else
                WriteObject(ref data);


            LogProgressBatch(data.Length);
        }

        private void WriteHeaderIfRequired()
        {
            if (!TypeInfo.IsArray && !TypeInfo.IsDynamic && Configuration.HasHeaderRecord)
            {
                CsvWriter.WriteHeader<TInput>();
                CsvWriter.NextRecord();
            }
        }

        private void WriteArray(ref TInput[] data)
        {
            foreach (var record in data)
            {
                if (record == null) continue;
                var recordAsArray = record as object[];
                try
                {
                    foreach (var field in recordAsArray)
                    {
                        CsvWriter.WriteField(field);
                    }
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(record));
                }

                CsvWriter.NextRecord();
            }
        }

        private void WriteObject(ref TInput[] data)
        {
            foreach (var record in data)
            {
                if (record == null) continue;
                try
                {
                    CsvWriter.WriteRecord(record);
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(record));
                }
                CsvWriter.NextRecord();
            }
        }

        public void CloseStreams()
        {
            CsvWriter?.Flush();
            StreamWriter?.Flush();
            CsvWriter?.Dispose();
            StreamWriter?.Close();
        }
    }

    /// <summary>
    /// A Csv destination defines a csv file where data from the flow is inserted. Inserts are done in batches (using Bulk insert).
    /// The CSVDestination uses a dynamic object as input type. If you need other data types, use the generic CSVDestination instead.
    /// </summary>
    /// <see cref="CsvDestination{TInput}"/>
    /// <example>
    /// <code>
    /// //Non generic CSVDestination works with dynamic object as input
    /// //use CSVDestination&lt;TInput&gt; for generic usage!
    /// CSVDestination dest = new CSVDestination("/path/to/file.csv");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class CsvDestination : CsvDestination<ExpandoObject>
    {
        public CsvDestination() : base() { }

        public CsvDestination(string fileName) : base(fileName) { }

    }

}
