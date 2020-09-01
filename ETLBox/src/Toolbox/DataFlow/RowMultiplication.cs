using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// This transformation allow you to transform one row of your input data into multiple rows.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    /// <typeparam name="TOutput">Type of outgoing data.</typeparam>
    public class RowMultiplication<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>, ILoggableTask, IDataFlowTransformation<TInput, TOutput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = $"Duplicate rows";
        /// <inheritdoc/>
        public override ISourceBlock<TOutput> SourceBlock => TransformBlock;
        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => TransformBlock;

        /// <summary>
        /// The transformation func that produces multiple rows for each ingoing row.
        /// </summary>
        public Func<TInput, IEnumerable<TOutput>> MultiplicationFunc { get; set; }

        #endregion

        #region Constructors

        public RowMultiplication()
        {

        }

        /// <param name="multiplicationFunc">Sets the <see cref="MultiplicationFunc"/></param>
        public RowMultiplication(Func<TInput, IEnumerable<TOutput>> multiplicationFunc) : this()
        {
            MultiplicationFunc = multiplicationFunc;
        }

        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {
            TransformBlock = new TransformManyBlock<TInput, TOutput>(MultiplicateRow, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize,
            });
        }

        protected override void CleanUpOnSuccess() {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        #endregion

        #region Implementation

        TransformManyBlock<TInput, TOutput> TransformBlock { get; set; }

        private IEnumerable<TOutput> MultiplicateRow(TInput row)
        {
            NLogStartOnce();
            if (row == null) return null;
            try
            {
                return MultiplicationFunc?.Invoke(row);
            }
            catch (Exception e)
            {
                ThrowOrRedirectError(e, ErrorSource.ConvertErrorData<TInput>(row));
                return default;
            }
        }

        #endregion
    }

    /// <inheritdoc/>
    public class RowMultiplication : RowMultiplication<ExpandoObject, ExpandoObject>
    {
        public RowMultiplication() : base()
        { }

        public RowMultiplication(Func<ExpandoObject, IEnumerable<ExpandoObject>> multiplicationFunc)
            : base(multiplicationFunc)
        { }
    }

    /// <inheritdoc/>
    public class RowMultiplication<TInput> : RowMultiplication<TInput, TInput>
    {
        public RowMultiplication() : base()
        { }

        public RowMultiplication(Func<TInput, IEnumerable<TInput>> multiplicationFunc)
            : base(multiplicationFunc)
        { }
    }
}
