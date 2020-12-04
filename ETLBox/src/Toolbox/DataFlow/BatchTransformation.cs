using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// A batch transformation will transform batches of data. The default batch size are 100000 rows. 
    /// The batch transformation function allows you to process and modify each batch of data. 
    /// You can use the BatchSize property to choose a smaller batch size. The batch size must always be smaller
    /// than the max buffer size. The default batch size are 1000 rows per batch.
    /// The batch transformation is a partial blocking transformation - it will always need at least enough
    /// memory to store a whole batch. 
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    /// <typeparam name="TOutput">Type of outgoing data.</typeparam>    
    public class BatchTransformation<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Excecute batch transformation";

        /// <inheritdoc/>
        public override ISourceBlock<TOutput> SourceBlock => OutputBuffer;
        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => InputBuffer;

        /// <summary>
        /// The transformation Func that is executed on each array of input data. It returns
        /// another array as output data - the output array can have a different length than
        /// the input array. 
        /// </summary>
        public Func<TInput[], TOutput[]> BatchTransformationFunc { get; set; }

        /// <summary>
        /// The size of each batch that is passed to the <see cref="BatchTransformation{TInput, TOutput}"/>
        /// </summary>
        public virtual int BatchSize { get; set; } = 1000;

        /// <summary>
        /// By default, all null values in the batch returned from the batch transformation func
        /// are filtered out. Set this option to true to avoid this behavior.
        /// </summary>
        public bool SuppressNullValueFilter { get; set; } 

        #endregion

        #region Constructors

        public BatchTransformation()
        {
        }

        /// <param name="batchSize">The size of each batch that is passed to the <see cref="BatchTransformation{TInput, TOutput}"/></param>
        public BatchTransformation(int batchSize) : this()
        {
            BatchSize = batchSize;
        }

        /// <param name="blockTransformationFunc">Sets the <see cref="BlockTransformationFunc"/></param>
        public BatchTransformation(int batchSize, Func<TInput[], TOutput[]> batchTransformationFunc) : this(batchSize)
        {
            BatchTransformationFunc = batchTransformationFunc;
        }

        #endregion

        #region Implement abstract methods

        protected override void CheckParameter()
        {
            if (BatchSize < 0)
                BatchSize = int.MaxValue;
            if (BatchSize == 0)
                throw new ETLBoxException("A batch size of 0 is not permitted!");
        }

        protected override void InitComponent()
        {
            InputBuffer = new BatchBlock<TInput>(BatchSize,
                new GroupingDataflowBlockOptions()
                {
                    BoundedCapacity = MaxBufferSize,
                    CancellationToken = this.BufferCancellationSource.Token

                });
            TransformationBlock = new ActionBlock<TInput[]>(this.ProcessBatch,
                new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = CalculateBatchedMaxBufferSize(),
                    CancellationToken = this.BufferCancellationSource.Token
                });
            OutputBuffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize,
                CancellationToken = this.BufferCancellationSource.Token
            });
            InputBuffer.LinkTo(TransformationBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            TransformationBlock.Completion.ContinueWith(t =>
            {
                if (t.IsFaulted)
                    ((IDataflowBlock)OutputBuffer).Fault(t.Exception.InnerException);
                else
                    OutputBuffer.Complete();
            });
        }

        private int CalculateBatchedMaxBufferSize()
        {
            if ( MaxBufferSize == -1) 
                return -1;
            else 
                return Math.Abs(MaxBufferSize / BatchSize) > 1 ? Math.Abs(MaxBufferSize / BatchSize) : 1;
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }


        protected override void CleanUpOnFaulted(Exception e) { }

        internal override void CompleteBuffer() => TargetBlock.Complete();

        internal override void FaultBuffer(Exception e) => TargetBlock.Fault(e);


        #endregion

        #region Implementation

        BufferBlock<TOutput> OutputBuffer;
        BatchBlock<TInput> InputBuffer;
        ActionBlock<TInput[]> TransformationBlock;

        private void ProcessBatch(TInput[] batch)
        {
            NLogStartOnce();
            try
            {
                TOutput[] batchoutput = BatchTransformationFunc.Invoke(batch);
                LogProgressBatch(BatchSize);
                foreach (TOutput row in batchoutput)
                {
                    if (!SuppressNullValueFilter && row == null) continue;
                    if (!OutputBuffer.SendAsync(row).Result)
                        HandleCanceledOrFaultedBuffer();
                }
            }
            catch (System.Threading.Tasks.TaskCanceledException) { throw; }
            catch (Exception e)
            {
                ThrowOrRedirectError(e, ErrorSource.ConvertErrorData<TInput[]>(batch));                
            }
        }
        
        #endregion
    }

    /// <inheritdoc/>
    public class BatchTransformation<TInput> : BatchTransformation<TInput, TInput>
    {
        public BatchTransformation() : base()
        { }
        public BatchTransformation(int batchSize) : base(batchSize)
        { }
        public BatchTransformation(int batchSize, Func<TInput[], TInput[]> batchTransformationFunc) : base(batchSize, batchTransformationFunc)
        { }
    }

    /// <inheritdoc/>
    public class BatchTransformation : BatchTransformation<ExpandoObject>
    {
        public BatchTransformation() : base()
        { }
        public BatchTransformation(int batchSize) : base(batchSize)
        { }
        public BatchTransformation(int batchSize, Func<ExpandoObject[], ExpandoObject[]> batchTransformationFunc) : base(batchSize, batchTransformationFunc)
        { }
    }

}
