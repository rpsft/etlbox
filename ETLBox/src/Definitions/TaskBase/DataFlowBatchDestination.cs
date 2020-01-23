using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public abstract class DataFlowBatchDestination<TInput> : DataFlowTask, ITask, IDataFlowDestination<TInput>
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
