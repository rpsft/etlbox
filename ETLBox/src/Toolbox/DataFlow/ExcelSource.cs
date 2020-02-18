using ALE.ETLBox.Helper;
using ExcelDataReader;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Reads data from a excel source. While reading the data from the file, data is also asnychronously posted into the targets.
    /// You can define a sheet name and a range - only the data in the specified sheet and range is read. Otherwise, all data
    /// in all sheets will be processed.
    /// </summary>
    /// <example>
    /// <code>
    /// ExcelSource&lt;ExcelData&gt; source = new ExcelSource&lt;ExcelData&gt;("src/DataFlow/ExcelDataFile.xlsx") {
    ///         Range = new ExcelRange(2, 4, 5, 9),
    ///         SheetName = "Sheet2"
    ///  };
    /// </code>
    /// </example>
    public class ExcelSource<TOutput> : DataFlowStreamSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName => $"Read excel data from file {Uri ?? ""}";

        /* Public properties */
        public string ExcelFilePassword { get; set; }
        public ExcelRange Range { get; set; }
        public string SheetName { get; set; }

        /* Private stuff */
        bool HasRange => Range != null;
        bool HasSheetName => !String.IsNullOrWhiteSpace(SheetName);
        IExcelDataReader ExcelDataReader { get; set; }
        ExcelTypeInfo TypeInfo { get; set; }
        public ExcelSource()
        {
            TypeInfo = new ExcelTypeInfo(typeof(TOutput));
            ResourceType = ResourceType.File;
        }

        public ExcelSource(string uri) : this()
        {
            Uri = uri;
        }

        protected override void ReadAll()
        {
            do
            {
                int rowNr = 0;

                while (ExcelDataReader.Read())
                {
                    if (ExcelDataReader.VisibleState != "visible") continue;
                    if (HasSheetName && ExcelDataReader.Name != SheetName) continue;
                    rowNr++;
                    if (HasRange && rowNr > Range.EndRowIfSet) break;
                    if (HasRange && rowNr < Range.StartRow) continue;
                    try
                    {
                        TOutput row = ParseDataRow();
                        if (row != null)
                            Buffer.SendAsync(row).Wait();
                    }
                    catch (Exception e)
                    {
                        if (!ErrorHandler.HasErrorBuffer) throw e;
                        ErrorHandler.Send(e, $"File: {Uri} -- Sheet: {SheetName ?? ""} -- Row: {rowNr}");
                    }
                    LogProgress();
                }
            } while (ExcelDataReader.NextResult());
        }

        private TOutput ParseDataRow()
        {
            TOutput row = (TOutput)Activator.CreateInstance(typeof(TOutput));
            bool emptyRow = true;
            for (int col = 0, colNrInRange = -1; col < ExcelDataReader.FieldCount; col++)
            {
                if (HasRange && col > Range.EndColumnIfSet) break;
                if (HasRange && (col + 1) < Range.StartColumn) continue;
                colNrInRange++;
                if (!TypeInfo.IsDynamic)
                    if (!TypeInfo.ExcelIndex2PropertyIndex.ContainsKey(colNrInRange)) { continue; }
                emptyRow &= ExcelDataReader.IsDBNull(col);
                object value = ExcelDataReader.GetValue(col);
                if (TypeInfo.IsDynamic)
                {
                    var r = row as IDictionary<string, Object>;
                    r.Add("Column"+ (colNrInRange+1), value);
                }
                else
                {
                    PropertyInfo propInfo = TypeInfo.Properties[TypeInfo.ExcelIndex2PropertyIndex[colNrInRange]];
                    propInfo.TrySetValue(row, TypeInfo.CastPropertyValue(propInfo, value?.ToString()));
                }
            }
            if (emptyRow) return default(TOutput);
            else return row;
        }

        protected override void InitReader()
        {
            ExcelDataReader = ExcelReaderFactory.CreateReader(StreamReader.BaseStream, new ExcelReaderConfiguration() { Password = ExcelFilePassword });
        }

        protected override void CloseReader()
        {
            ExcelDataReader.Close();
        }
    }

    /// <summary>
    /// Reads data from a excel source. While reading the data from the file, data is also asnychronously posted into the targets.
    /// You can define a sheet name and a range - only the data in the specified sheet and range is read. Otherwise, all data
    /// in all sheets will be processed.
    /// The non generic class uses a dynamic object for storing the data. 
    /// </summary>
    /// <see cref="ExcelSource{TOutput}"/>
    /// <example>
    /// <code>
    /// ExcelSourcesource = new ExcelSource("src/DataFlow/ExcelDataFile.xlsx") {
    ///         Range = new ExcelRange(2, 4, 5, 9),
    ///         SheetName = "Sheet2"
    ///  };
    /// </code>
    /// </example>
    public class ExcelSource : ExcelSource<ExpandoObject>
    {
        public ExcelSource() : base()
        { }

        public ExcelSource(string uri) : base(uri)
        { }
    }
}
