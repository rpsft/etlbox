using CsvHelper;
using CsvHelper.Configuration;
using System;
using System.Globalization;
using System.IO;
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
    public class CSVSource<TOutput> : DataFlowSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName => $"Read CSV data from file {FileName ?? ""}";

        /* Public properties */
        public Configuration Configuration { get; set; }
        public int SkipRows { get; set; } = 0;
        public string FileName { get; set; }
        public string[] FieldHeaders { get; private set; }
        public bool IsHeaderRead => FieldHeaders != null;

        /* Private stuff */
        CsvReader CsvReader { get; set; }
        StreamReader StreamReader { get; set; }

        public CSVSource() : base()
        {
            Configuration = new Configuration(CultureInfo.InvariantCulture);
        }

        public CSVSource(string fileName) : this()
        {
            FileName = fileName;
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
            StreamReader = new StreamReader(FileName, Configuration.Encoding ?? Encoding.UTF8, true);
            SkipFirstRows();
            CsvReader = new CsvReader(StreamReader, Configuration);
        }

        private void SkipFirstRows()
        {
            for (int i = 0; i < SkipRows; i++)
                StreamReader.ReadLine();
        }


        private void ReadAll()
        {
            if (Configuration.HasHeaderRecord == true)
            {
                CsvReader.Read();
                CsvReader.ReadHeader();
                FieldHeaders = CsvReader.Context.HeaderRecord;
            }
            while (CsvReader.Read())
            {
                ReadLineAndSendIntoBuffer();
                LogProgress(1);
            }
        }

        private void ReadLineAndSendIntoBuffer()
        {
            if (TypeInfo.IsArray)
            {
                string[] line = CsvReader.Context.Record;
                Buffer.Post((TOutput)(object)line);
            }
            else if (TypeInfo.IsDynamic)
            {
                TOutput bufferObject = CsvReader.GetRecord<dynamic>();
                Buffer.Post(bufferObject);
            }
            else
            {
                TOutput bufferObject = CsvReader.GetRecord<TOutput>();
                Buffer.Post(bufferObject);
            }
        }

        private void Close()
        {
            CsvReader?.Dispose();
            StreamReader?.Dispose();
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
