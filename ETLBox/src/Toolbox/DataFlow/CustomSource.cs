using System;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Define your own source block.
    /// </summary>
    /// <typeparam name="TOutput">Type of data output.</typeparam>
    public class CustomSource<TOutput> : DataFlowTask, ITask, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_CUSTOMSOURCE";
        public override string TaskName => $"Dataflow: Custom source";
        public override void Execute() => ExecuteAsync();

        /* Public properties */
        public ISourceBlock<TOutput> SourceBlock => this.Buffer;
        public Func<TOutput> ReadFunc { get; set; }
        public Func<bool> ReadCompletedFunc { get; set; }

        /* Private stuff */
        BufferBlock<TOutput> Buffer { get; set; }
        NLog.Logger NLogger { get; set; }

        public CustomSource()
        {
            NLogger = NLog.LogManager.GetLogger("ETL");
            Buffer = new BufferBlock<TOutput>();
        }

        public CustomSource(Func<TOutput> readFunc, Func<bool> readCompletedFunc) : this()
        {
            ReadFunc = readFunc;
            ReadCompletedFunc = readCompletedFunc;
        }

        public CustomSource(string name, Func<TOutput> readFunc, Func<bool> readCompletedFunc) : this(readFunc, readCompletedFunc)
        {
            this.TaskName = name;
        }

        public void ExecuteAsync()
        {
            NLogStart();
            while (!ReadCompletedFunc.Invoke())
            {
                Buffer.Post(ReadFunc.Invoke());
                LogProgress(1);
            }
            Buffer.Complete();
            NLogFinish();
        }


        public void LinkTo(IDataFlowLinkTarget<TOutput> target)
        {
            Buffer.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            if (!DisableLogging)
                NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        public void LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
        {
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

    /// <summary>
    /// Define your own source block. The non generic implementation return a string array as output.
    /// </summary>
    public class CustomSource : CustomSource<string[]>
    {

        public CustomSource() : base()
        { }

        public CustomSource(Func<string[]> readFunc, Func<bool> readCompletedFunc) : base(readFunc, readCompletedFunc)
        { }

        public CustomSource(string name, Func<string[]> readFunc, Func<bool> readCompletedFunc) : base(name, readFunc, readCompletedFunc)
        { }
    }
}
