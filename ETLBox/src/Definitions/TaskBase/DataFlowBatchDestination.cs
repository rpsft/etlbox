using ETLBox.ControlFlow;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowBatchDestination<TInput> : DataFlowDestination<TInput[]>, ITask, IDataFlowBatchDestination<TInput>
    {
        /// <summary>
        /// This function is called every time before a batch is inserted into the destination.
        /// It receives an array that represents the batch - you can modify the data itself if needed.
        /// </summary>
        public Func<TInput[], TInput[]> BeforeBatchWrite { get; set; }
        /// <summary>
        /// This action is called after a batch was successfully inserted into the destination.
        /// You will get a copy of the data that was used for the insertion.
        /// </summary>
        public Action<TInput[]> AfterBatchWrite { get; set; }
        /// <summary>
        /// The buffer component used as target for linking.
        /// </summary>
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
                InitBufferObjects();
            }
        }
        private int batchSize;

        public const int DEFAULT_BATCH_SIZE = 1000;

        protected bool WasInitialized { get; set; }

        /// <summary>
        /// Use this method if you want to register a task that needs to be completed
        /// before the destination itself can complete. Normally you don't have to do anything -
        /// all linked components will automatically register using this method.
        /// Simple use the LinkTo() method of source components or transformations.
        /// </summary>
        /// <param name="completion">A task to wait for before this destination can complete.</param>
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

        protected override void InitBufferObjects()
        {
            Buffer = new BatchBlock<TInput>(BatchSize, new GroupingDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
            int boundedCapacity = -1;
            if (MaxBufferSize > 0 && BatchSize > 0)
                boundedCapacity = Math.Abs(MaxBufferSize / BatchSize);
            TargetAction = new ActionBlock<TInput[]>(WriteBatch, new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = 1, //No parallel inserts on Db!
                BoundedCapacity = boundedCapacity
            });
            SetCompletionTask();
            Buffer.LinkTo(TargetAction, new DataflowLinkOptions() { PropagateCompletion = true });
        }

        protected void WriteBatch(TInput[] data)
        {
            if (ProgressCount == 0) NLogStart();
            if (BeforeBatchWrite != null)
                data = BeforeBatchWrite.Invoke(data);
            if (!WasInitialized)
            {
                PrepareWrite();
                WasInitialized = true;
            }
            TryBulkInsertData(data);
            LogProgressBatch(data.Length);
            AfterBatchWrite?.Invoke(data);
        }

        protected override void CleanUp()
        {
            FinishWrite();
            base.CleanUp();
        }

        protected abstract void PrepareWrite();
        protected abstract void TryBulkInsertData(TInput[] data);
        protected abstract void FinishWrite();

    }
}
