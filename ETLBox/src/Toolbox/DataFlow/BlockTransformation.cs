using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// A block transformation will wait for all data from the flow to be loaded into its buffer.
    /// After all data is in the buffer, the transformation function
    /// is executed for the complete data and the result posted into the targets.
    /// The block transformations allows you to access all data in the flow in one generic collection.
    /// But as this block any processing until all data is buffered, it will also need to store the whole data in memory.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    /// <typeparam name="TOutput">Type of outgoing data.</typeparam>
    /// <example>
    /// <code>
    /// BlockTransformation&lt;MyInputRow, MyOutputRow&gt; block = new BlockTransformation&lt;MyInputRow, MyOutputRow&gt;(
    ///     inputDataAsList => {
    ///         return inputData.Select( inputRow => new MyOutputRow() { Value2 = inputRow.Value1 }).ToList();
    ///     });
    /// </code>
    /// </example>
    public class BlockTransformation<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Excecute block transformation";
        /// <summary>
        /// The transformation Func that is executed on the all input data. It should return a new list that
        /// contains all output data for further processing.
        /// </summary>
        public Func<List<TInput>, List<TOutput>> BlockTransformationFunc { get; set; }
        /// <inheritdoc/>
        public override ISourceBlock<TOutput> SourceBlock => OutputBuffer;
        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => InputBuffer;

        #endregion

        #region Constructors

        public BlockTransformation()
        {
            InputData = new List<TInput>();
        }

        /// <param name="blockTransformationFunc">Sets the <see cref="BlockTransformationFunc"/></param>
        public BlockTransformation(Func<List<TInput>, List<TOutput>> blockTransformationFunc) : this()
        {
            BlockTransformationFunc = blockTransformationFunc;
        }

        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {
            OutputBuffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
            InputBuffer = new ActionBlock<TInput>(row => InputData.Add(row),
                new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = -1 //All data is always loaded!
                });
            InputBuffer.Completion.ContinueWith(t =>
            {
                if (t.IsFaulted) ((IDataflowBlock)OutputBuffer).Fault(t.Exception.InnerException);
                try
                {
                    NLogStartOnce();
                    WriteIntoOutput();
                    OutputBuffer.Complete();
                }
                catch (Exception e)
                {
                    ((IDataflowBlock)OutputBuffer).Fault(e);
                    throw e;
                }
            });
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }


        protected override void CleanUpOnFaulted(Exception e) { }

        internal override void CompleteBufferOnPredecessorCompletion() => TargetBlock.Complete();

        internal override void FaultBufferOnPredecessorCompletion(Exception e) => TargetBlock.Fault(e);


        #endregion

        #region Implementation

        BufferBlock<TOutput> OutputBuffer;
        ActionBlock<TInput> InputBuffer;
        List<TInput> InputData;
        List<TOutput> OutputData;

        private void WriteIntoOutput()
        {
            OutputData = BlockTransformationFunc(InputData);
            foreach (TOutput row in OutputData)
            {
                if (!OutputBuffer.SendAsync(row).Result)
                    throw new ETLBoxException("Buffer already completed or faulted!", this.Exception);
                LogProgress();
            }
        }

        #endregion
    }

    /// <inheritdoc/>
    public class BlockTransformation<TInput> : BlockTransformation<TInput, TInput>
    {
        public BlockTransformation() : base()
        { }
        public BlockTransformation(Func<List<TInput>, List<TInput>> blockTransformationFunc) : base(blockTransformationFunc)
        { }

    }

    /// <inheritdoc/>
    public class BlockTransformation : BlockTransformation<ExpandoObject>
    {
        public BlockTransformation() : base()
        { }

        public BlockTransformation(Func<List<ExpandoObject>, List<ExpandoObject>> blockTransformationFunc) : base(blockTransformationFunc)
        { }

    }

}
