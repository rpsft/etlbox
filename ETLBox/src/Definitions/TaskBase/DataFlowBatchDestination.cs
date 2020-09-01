using ETLBox.ControlFlow;
using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowBatchDestination<TInput> : DataFlowComponent,  IDataFlowBatchDestination<TInput>, IDataFlowDestination<TInput>
    {
        #region Public properties

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
        public ITargetBlock<TInput> TargetBlock => Buffer;
        /// <summary>
        /// The batch size defines how many records needs to be in the Input buffer before data is written into the destination.
        /// The default batch size is 1000.
        /// </summary>
        public int BatchSize { get; set; } = DEFAULT_BATCH_SIZE;

        public const int DEFAULT_BATCH_SIZE = 1000;

        #endregion

        #region Implement abstract methods

        public void Wait() => Completion.Wait();

        protected ActionBlock<TInput[]> TargetAction { get; set; }

        internal override Task BufferCompletion => TargetAction.Completion;

        internal override void CompleteBufferOnPredecessorCompletion()
        {

            TargetBlock.Complete();
        }

        internal override void FaultBufferOnPredecessorCompletion(Exception e) => TargetBlock.Fault(e);

        public IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
            => InternalLinkErrorTo(target);

        protected override void InternalInitBufferObjects()
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
            Buffer.LinkTo(TargetAction, new DataflowLinkOptions() { PropagateCompletion = true });
        }

        protected override void CleanUpOnSuccess()
        {
            FinishWrite();
            NLogFinishOnce();
        }
        protected override void CleanUpOnFaulted(Exception e)
        {
            FinishWrite();
        }

        #endregion

        #region Implementation

        protected bool WasWritingPrepared { get; set; }

        protected BatchBlock<TInput> Buffer { get; set; }

        protected void WriteBatch(TInput[] data)
        {
            if (ProgressCount == 0) NLogStartOnce();
            if (BeforeBatchWrite != null)
                data = BeforeBatchWrite.Invoke(data);
            if (!WasWritingPrepared)
            {
                PrepareWrite();
                WasWritingPrepared = true;
            }
            TryBulkInsertData(data);
            LogProgressBatch(data.Length);
            AfterBatchWrite?.Invoke(data);
        }

        protected abstract void PrepareWrite();
        protected abstract void TryBulkInsertData(TInput[] data);
        protected abstract void FinishWrite();

        #endregion
    }
}
