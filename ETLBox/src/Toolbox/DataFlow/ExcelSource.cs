using CsvHelper;
using ExcelDataReader;
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow {
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
    public class ExcelSource<TOutput> : DataFlowTask, ITask, IDataFlowSource<TOutput> where TOutput : new() {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_EXCELSOURCE";
        public override string TaskName => $"Dataflow: Read Excel source data from file {FileName}";
        public override void Execute() => ExecuteAsync();

        /* Public properties */
        public ISourceBlock<TOutput> SourceBlock => this.Buffer;
        public string FileName { get; set; }
        public string ExcelFilePassword { get; set; }
        public ExcelRange Range { get; set; }
        public bool HasRange => Range != null;
        public string SheetName { get; set; }
        public bool HasSheetName => !String.IsNullOrWhiteSpace(SheetName);
        /* Private stuff */
        FileStream FileStream { get; set; }
        IExcelDataReader ExcelDataReader { get; set; }
        BufferBlock<TOutput> Buffer { get; set; }

        public ExcelSource() {
            Buffer = new BufferBlock<TOutput>();
        }

        public ExcelSource(string fileName) : this() {
            FileName = fileName;
        }

        public void ExecuteAsync() {
            NLogStart();
            Open();
            try {
                ReadAll().Wait();
                Buffer.Complete();
            } catch (Exception e) {
                throw new ETLBoxException("Error during reading data from excel file - see inner exception for details.", e);
            } finally {
                Close();
            }
            NLogFinish();
        }

        private async Task ReadAll() {
            do {
                int rowNr = 0;
                TypeInfo typeInfo = new TypeInfo(typeof(TOutput));
                while (ExcelDataReader.Read()) {
                    if (ExcelDataReader.VisibleState != "visible") continue;
                    if (HasSheetName && ExcelDataReader.Name != SheetName) continue;
                    rowNr++;
                    if (HasRange && rowNr > Range.EndRowIfSet) break;
                    if (HasRange && rowNr < Range.StartRow) continue;
                    TOutput row = ParseDataRow(typeInfo);
                    await Buffer.SendAsync(row);
                    LogProgress(1);
                }
            } while (ExcelDataReader.NextResult());


        }

        private TOutput ParseDataRow(TypeInfo typeInfo) {
            TOutput row = new TOutput();
            int colInRange = 0;
            for (int col = 0; col < ExcelDataReader.FieldCount; col++) {
                if (HasRange && col > Range.EndColumnIfSet) break;
                if (HasRange && (col + 1) < Range.StartColumn) continue;
                if (colInRange > typeInfo.PropertyLength) break;
                if (!typeInfo.ExcelIndex2PropertyIndex.ContainsKey(colInRange)) continue;
                PropertyInfo propInfo = typeInfo.Properties[typeInfo.ExcelIndex2PropertyIndex[colInRange]];
                object value = ExcelDataReader.GetValue(col);
                propInfo.SetValue(row, TypeInfo.CastPropertyValue(propInfo, value?.ToString()));
                colInRange++;
            }
            return row;
        }

        private void Open() {
            FileStream = File.Open(FileName, FileMode.Open, FileAccess.Read);
            ExcelDataReader = ExcelReaderFactory.CreateReader(FileStream, new ExcelReaderConfiguration() { Password = ExcelFilePassword });
        }

        private void Close() {
            ExcelDataReader.Close();

        }
        public void LinkTo(IDataFlowLinkTarget<TOutput> target) {
            Buffer.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            if (!DisableLogging)
                NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        public void LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate) {
            Buffer.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true }, predicate);
            if (!DisableLogging)
                NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
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
            if (!DisableLogging && HasLoggingThresholdRows && (ProgressCount % LoggingThresholdRows == 0))
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }
    }
}
