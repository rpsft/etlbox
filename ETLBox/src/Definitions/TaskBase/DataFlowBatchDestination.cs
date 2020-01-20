using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public abstract class DataFlowBatchDestination<TInput> : DataFlowTask, ITask
    {
        public Func<TInput[], TInput[]> BeforeBatchWrite { get; set; }
        public Action OnCompletion { get; set; }
        public Task Completion { get; private set; }
        public ITargetBlock<TInput> TargetBlock => Buffer;

        public int BatchSize
        {
            get { return batchSize; }
            set
            {
                batchSize = value;
                InitObjects(batchSize);
            }
        }
        private int batchSize;

        internal Action CloseStreamsAction { get; set; }
        internal BatchBlock<TInput> Buffer { get; set; }
        internal ActionBlock<TInput[]> TargetAction { get; set; }
        internal int ThresholdCount { get; set; } = 1;
        internal TypeInfo TypeInfo { get; set; }
        internal ErrorHandler ErrorHandler { get; set; } = new ErrorHandler();

        internal virtual void InitObjects(int batchSize)
        {
            Buffer = new BatchBlock<TInput>(batchSize);
            TargetAction = new ActionBlock<TInput[]>(d => WriteBatch(ref d));
            Completion = AwaitCompletion();
            Buffer.LinkTo(TargetAction, new DataflowLinkOptions() { PropagateCompletion = true });
            TypeInfo = new TypeInfo(typeof(TInput));
        }

        internal virtual void WriteBatch(ref TInput[] data)
        {
            if (ProgressCount == 0) NLogStart();
            if (BeforeBatchWrite != null)
                data = BeforeBatchWrite.Invoke(data);
        }

        internal void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

        internal void NLogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                NLogger.Info(TaskName + $" processed {ProgressCount} records in total.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
        }

        internal void LogProgress(int rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (!DisableLogging && HasLoggingThresholdRows && ProgressCount >= (LoggingThresholdRows * ThresholdCount))
            {
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
                ThresholdCount++;
            }
        }

        public virtual void Wait()
        {
            Completion.Wait();
        }

        internal async Task AwaitCompletion()
        {
            await TargetAction.Completion.ConfigureAwait(false);
            CleanUp();
        }

        private void CleanUp()
        {
            CloseStreamsAction?.Invoke();
            OnCompletion?.Invoke();
            NLogFinish();
        }

        public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target)
            => ErrorHandler.LinkErrorTo(target, TargetAction.Completion);

    }
}
