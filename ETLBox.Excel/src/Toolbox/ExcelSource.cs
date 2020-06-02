using ETLBox.DataFlow;
using ETLBox.Helper;
using ExcelDataReader;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks.Dataflow;
using TheBoxOffice.LicenseManager;

namespace ETLBox.Excel
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
        public override string TaskName => $"Read excel data from Uri: {CurrentRequestUri ?? ""}";

        /* Public properties */
        public string ExcelFilePassword { get; set; }
        public ExcelRange Range { get; set; }
        public string SheetName { get; set; }
        public bool IgnoreBlankRows { get; set; }
        public bool HasNoHeader { get; set; }

        /* Private stuff */
        List<string> HeaderColumns = new List<string>();
        bool HasHeaderData => !HasNoHeader && HeaderColumns?.Count > 0;
        bool HasRange => Range != null;
        bool HasSheetName => !String.IsNullOrWhiteSpace(SheetName);
        bool IsHeaderRead { get; set; }
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
                        if (!HasNoHeader && !IsHeaderRead)
                            ParseHeader();
                        else
                        {
                            TOutput row = ParseDataRow();
                            if (row == null && IgnoreBlankRows) continue;
                            else if (row == null && !IgnoreBlankRows) break;
                            else if (row != null)
                            {
                                Buffer.SendAsync(row).Wait();
                                LogProgress();
                                if (ProgressCount > 0 && ProgressCount % LicenseCheck.FreeRows == 0)
                                    LicenseCheck.CheckValidLicenseOrThrow();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        if (!ErrorHandler.HasErrorBuffer) throw e;
                        ErrorHandler.Send(e, $"File: {Uri} -- Sheet: {SheetName ?? ""} -- Row: {rowNr}");
                    }

                }
            } while (ExcelDataReader.NextResult());
        }

        private void ParseHeader()
        {
            for (int col = 0, colNrInRange = -1; col < ExcelDataReader.FieldCount; col++)
            {
                if (HasRange && col > Range.EndColumnIfSet) break;
                if (HasRange && (col + 1) < Range.StartColumn) continue;
                colNrInRange++;
                string value = Convert.ToString(ExcelDataReader.GetValue(col));
                HeaderColumns.Add(value);
            }
            IsHeaderRead = true;
        }

        private TOutput ParseDataRow()
        {
            TOutput row;
            row = GetNewOutputInstance();
            bool emptyRow = true;
            for (int col = 0, colNrInRange = -1; col < ExcelDataReader.FieldCount; col++)
            {
                if (HasRange && col > Range.EndColumnIfSet) break;
                if (HasRange && (col + 1) < Range.StartColumn) continue;
                colNrInRange++;
                emptyRow &= ExcelDataReader.IsDBNull(col);
                object value = ExcelDataReader.GetValue(col);
                if (TypeInfo.IsArray)
                    SetValueInArray(row, colNrInRange, value);
                else if (TypeInfo.IsDynamic)
                    SetValueInDynamic(row, colNrInRange, value);
                else
                    SetValueInObject(row, colNrInRange, value);
            }
            if (emptyRow) return default(TOutput);
            else return row;
        }

        private TOutput GetNewOutputInstance()
        {
            if (TypeInfo.IsArray)
                return (TOutput)Activator.CreateInstance(typeof(TOutput), new object[] { ExcelDataReader.FieldCount });
            else
                return (TOutput)Activator.CreateInstance(typeof(TOutput));
        }

        private void SetValueInArray(TOutput row, int colNrInRange, object value)
        {
            var ar = row as System.Array;
            var con = Convert.ChangeType(value, typeof(TOutput).GetElementType());
            ar.SetValue(con, colNrInRange);
        }

        private void SetValueInDynamic(TOutput row, int colNrInRange, object value)
        {
            var r = row as IDictionary<string, Object>;
            if (HasHeaderData)
                r.Add(HeaderColumns[colNrInRange], value);
            else
                r.Add("Column" + (colNrInRange + 1), value);
        }

        private void SetValueInObject(TOutput row, int colNrInRange, object value)
        {
            PropertyInfo propInfo = null;
            if (HasHeaderData && TypeInfo.ExcelColumnName2PropertyIndex.ContainsKey(HeaderColumns[colNrInRange]))
                propInfo = TypeInfo.Properties[TypeInfo.ExcelColumnName2PropertyIndex[HeaderColumns[colNrInRange]]];
            else if (TypeInfo.ExcelIndex2PropertyIndex.ContainsKey(colNrInRange))
                propInfo = TypeInfo.Properties[TypeInfo.ExcelIndex2PropertyIndex[colNrInRange]];
            propInfo?.TrySetValue(row, TypeInfo.CastPropertyValue(propInfo, value?.ToString()));
        }

        protected override void InitReader()
        {
            ExcelDataReader = ExcelReaderFactory.CreateReader(StreamReader.BaseStream, new ExcelReaderConfiguration() { Password = ExcelFilePassword });
        }

        protected override void CloseReader()
        {
            ExcelDataReader?.Close();
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
