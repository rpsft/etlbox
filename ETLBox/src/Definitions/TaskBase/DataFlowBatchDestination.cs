using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public abstract class DataFlowBatchDestination<TInput> : DataFlowDestination<TInput[]>, ITask, IDataFlowDestination<TInput>
    {
        public Func<TInput[], TInput[]> BeforeBatchWrite { get; set; }
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

        protected Action CloseStreamsAction { get; set; }
        protected BatchBlock<TInput> Buffer { get; set; }
        internal TypeInfo TypeInfo { get; set; }
        internal ErrorHandler ErrorHandler { get; set; } = new ErrorHandler();

        public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target)
            => ErrorHandler.LinkErrorTo(target, TargetAction.Completion);

        protected virtual void InitObjects(int batchSize)
        {
            Buffer = new BatchBlock<TInput>(batchSize);
            TargetAction = new ActionBlock<TInput[]>(d => WriteBatch(ref d));
            SetCompletionTask();
            Buffer.LinkTo(TargetAction, new DataflowLinkOptions() { PropagateCompletion = true });
            TypeInfo = new TypeInfo(typeof(TInput));
        }

        protected virtual void WriteBatch(ref TInput[] data)
        {
            if (ProgressCount == 0) NLogStart();
            if (BeforeBatchWrite != null)
                data = BeforeBatchWrite.Invoke(data);
        }

        protected override void CleanUp()
        {
            CloseStreamsAction?.Invoke();
            OnCompletion?.Invoke();
            NLogFinish();
        }

    }
}
