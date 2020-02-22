using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Dynamic;
using System.Globalization;
using System.IO;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A Csv destination defines a csv file where data from the flow is inserted.
    /// </summary>
    /// <see cref="DbDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    /// <example>
    /// <code>
    /// CsvDestination&lt;MyRow&gt; dest = new CsvDestination&lt;MyRow&gt;("/path/to/file.csv");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class CsvDestination<TInput> : DataFlowStreamDestination<TInput>, ITask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName => $"Write Csv data into file {Uri ?? ""}";
        public Configuration Configuration { get; set; }

        CsvWriter CsvWriter { get; set; }
        TypeInfo TypeInfo { get; set; }

        public CsvDestination()
        {
            Configuration = new Configuration(CultureInfo.InvariantCulture);
            Configuration.TypeConverterOptionsCache.GetOptions<DateTime>().Formats = new[] { "yyyy-MM-dd HH:mm:ss.fff" };
            TypeInfo = new TypeInfo(typeof(TInput));
            ResourceType = ResourceType.File;
            InitTargetAction();
        }

        public CsvDestination(string uri) : this()
        {
            Uri = uri;
        }

        protected override void InitStream()
        {
            CsvWriter = new CsvWriter(StreamWriter, Configuration, leaveOpen: true);
            WriteHeaderIfRequired();
        }

        protected override void WriteIntoStream(TInput data)
        {
            if (TypeInfo.IsArray)
                WriteArray(data);
            else
                WriteObject(data);

            LogProgress();
        }

        private void WriteHeaderIfRequired()
        {
            if (!TypeInfo.IsArray && !TypeInfo.IsDynamic && Configuration.HasHeaderRecord)
            {
                CsvWriter.WriteHeader<TInput>();
                CsvWriter.NextRecord();
            }
        }

        private void WriteArray(TInput data)
        {
            if (data == null) return;
            var recordAsArray = data as object[];
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
                ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(data));
            }

            CsvWriter.NextRecord();
        }

        private void WriteObject(TInput data)
        {
            if (data == null) return;
            try
            {
                CsvWriter.WriteRecord(data);
            }
            catch (Exception e)
            {
                if (!ErrorHandler.HasErrorBuffer) throw e;
                ErrorHandler.Send(e, ErrorHandler.ConvertErrorData(data));
            }
            CsvWriter.NextRecord();
        }

        protected override void CloseStream()
        {
            CsvWriter?.Flush();
            CsvWriter?.Dispose();
        }
    }

    /// <summary>
    /// A Csv destination defines a csv file where data from the flow is inserted. 
    /// The CsvDestination uses a dynamic object as input type. If you need other data types, use the generic CsvDestination instead.
    /// </summary>
    /// <see cref="CsvDestination{TInput}"/>
    /// <example>
    /// <code>
    /// //Non generic CsvDestination works with dynamic object as input
    /// //use CsvDestination&lt;TInput&gt; for generic usage!
    /// CsvDestination dest = new CsvDestination("/path/to/file.csv");
    /// dest.Wait(); //Wait for all data to arrive
    /// </code>
    /// </example>
    public class CsvDestination : CsvDestination<ExpandoObject>
    {
        public CsvDestination() : base() { }

        public CsvDestination(string fileName) : base(fileName) { }

    }

}
