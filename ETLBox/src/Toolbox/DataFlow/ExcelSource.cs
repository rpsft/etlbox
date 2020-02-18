using ALE.ETLBox.Helper;
using ExcelDataReader;
using System;
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
    public class ExcelSource<TOutput> : DataFlowSource<TOutput>, ITask, IDataFlowSource<TOutput> where TOutput : new()
    {
        /* ITask Interface */
        public override string TaskName => $"Read excel data from file {FileName ?? ""}";

        /* Public properties */
        public string FileName { get; set; }
        public string ExcelFilePassword { get; set; }
        public ExcelRange Range { get; set; }
        public bool HasRange => Range != null;
        public string SheetName { get; set; }
        public bool HasSheetName => !String.IsNullOrWhiteSpace(SheetName);
        /* Private stuff */
        FileStream FileStream { get; set; }
        IExcelDataReader ExcelDataReader { get; set; }
        ExcelTypeInfo TypeInfo { get; set; }
        public ExcelSource()
        {
            TypeInfo = new ExcelTypeInfo(typeof(TOutput));
        }

        public ExcelSource(string fileName) : this()
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
            finally
            {
                Close();
            }
            NLogFinish();
        }

        private void ReadAll()
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
                        ErrorHandler.Send(e, $"File: {FileName} -- Sheet: {SheetName ?? ""} -- Row: {rowNr}");
                    }
                    LogProgress();
                }
            } while (ExcelDataReader.NextResult());
        }

        private TOutput ParseDataRow()
        {
            TOutput row = new TOutput();
            bool emptyRow = true;
            for (int col = 0 ,colNrInRange = -1; col < ExcelDataReader.FieldCount; col++)
            {
                if (HasRange && col > Range.EndColumnIfSet) break;
                if (HasRange && (col + 1) < Range.StartColumn) continue;
                colNrInRange++;
                if (!TypeInfo.ExcelIndex2PropertyIndex.ContainsKey(colNrInRange)) {  continue; }
                PropertyInfo propInfo = TypeInfo.Properties[TypeInfo.ExcelIndex2PropertyIndex[colNrInRange]];
                emptyRow &= ExcelDataReader.IsDBNull(col);
                object value = ExcelDataReader.GetValue(col);
                propInfo.TrySetValue(row, TypeInfo.CastPropertyValue(propInfo, value?.ToString()));
            }
            if (emptyRow) return default(TOutput);
            else return row;
        }

        private void Open()
        {
            FileStream = File.Open(FileName, FileMode.Open, FileAccess.Read);
            ExcelDataReader = ExcelReaderFactory.CreateReader(FileStream, new ExcelReaderConfiguration() { Password = ExcelFilePassword });
        }

        private void Close()
        {
            ExcelDataReader.Close();
        }
    }
}
