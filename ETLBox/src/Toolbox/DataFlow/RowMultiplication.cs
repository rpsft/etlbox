using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// This transformation allow you to transform your input data into multple output data records.
    /// </summary>
    /// <typeparam name="TInput">Type of data input</typeparam>
    /// <typeparam name="TOutput">Type of data output</typeparam>
    public class RowMultiplication<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>, ITask, IDataFlowTransformation<TInput, TOutput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = $"Duplicate rows.";

        /* Public Properties */
        public override ISourceBlock<TOutput> SourceBlock => TransformBlock;
        public override ITargetBlock<TInput> TargetBlock => TransformBlock;
        public Func<TInput, IEnumerable<TOutput>> MultiplicationFunc { get; set; }


        /* Private stuff */
        TransformManyBlock<TInput, TOutput> TransformBlock { get; set; }

        internal ErrorHandler ErrorHandler { get; set; } = new ErrorHandler();

        public RowMultiplication()
        {
            InitBufferObjects();
        }

        public RowMultiplication(Func<TInput, IEnumerable<TOutput>> multiplicationFunc) : this()
        {
            MultiplicationFunc = multiplicationFunc;
        }

        protected override void InitBufferObjects()
        {
            TransformBlock = new TransformManyBlock<TInput, TOutput>(MultiplicateRow, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize,
            });
        }

        private IEnumerable<TOutput> MultiplicateRow(TInput row)
        {
            if (row == null) return null;
            try
            {
                return MultiplicationFunc?.Invoke(row);
            }
            catch (Exception e)
            {
                if (!ErrorHandler.HasErrorBuffer) throw e;
                ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TInput>(row));
                return null;
            }
        }

        public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target)
            => ErrorHandler.LinkErrorTo(target, TransformBlock.Completion);

    }

    /// <summary>
    /// This transformation allow you to transform your input data into multple output data records.
    /// </summary>
    /// <see cref="RowMultiplication{TInput, TOutput}"/>
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
