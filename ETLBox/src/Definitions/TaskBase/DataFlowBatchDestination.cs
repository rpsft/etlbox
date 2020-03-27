using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public abstract class DataFlowBatchDestination<TInput> : DataFlowDestination<TInput[]>, ITask, IDataFlowDestination<TInput>
    {
        /// <summary>
        /// This function is called every time before a batch is inserted into the destination. 
        /// It receives an array that represents the batch - you can modify the data itself if needed. 
        /// </summary>
        public Func<TInput[], TInput[]> BeforeBatchWrite { get; set; }
        public Action<TInput[]> AfterBatchWrite { get; set; }
        public new ITargetBlock<TInput> TargetBlock => Buffer;
        /// <summary>
        /// The batch size defines how many records needs to be in the Input buffer before data is written into the destination.
        /// The default batch size is 1000.
        /// </summary>
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

        public new void AddPredecessorCompletion(Task completion)
        {
            PredecessorCompletions.Add(completion);
            completion.ContinueWith(t => CheckCompleteAction());
        }

        protected new void CheckCompleteAction()
        {
            Task.WhenAll(PredecessorCompletions).ContinueWith(t =>
            {
                if (!TargetBlock.Completion.IsCompleted)
                {
                    if (t.IsFaulted) TargetBlock.Fault(t.Exception.InnerException);
                    else TargetBlock.Complete();
                }
            });
        }

        protected BatchBlock<TInput> Buffer { get; set; }

        protected virtual void InitObjects(int batchSize)
        {
            Buffer = new BatchBlock<TInput>(batchSize);
            TargetAction = new ActionBlock<TInput[]>(WriteBatch);
            SetCompletionTask();
            Buffer.LinkTo(TargetAction, new DataflowLinkOptions() { PropagateCompletion = true });
        }

        protected void WriteBatch(TInput[] data)
        {
            if (ProgressCount == 0) NLogStart();
            if (BeforeBatchWrite != null)
                data = BeforeBatchWrite.Invoke(data);
            TryBulkInsertData(data);
            LogProgressBatch(data.Length);
            if(AfterBatchWrite != null)
                AfterBatchWrite.Invoke(data);
        }

        protected abstract void TryBulkInsertData(TInput[] data);
    }
}
