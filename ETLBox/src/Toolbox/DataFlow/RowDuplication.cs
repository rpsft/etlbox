using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// Creates one or more duplicates of your incoming row.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    public class RowDuplication<TInput> : DataFlowTransformation<TInput, TInput>, ILoggableTask, IDataFlowTransformation<TInput, TInput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = $"Duplicate rows.";

        /// <summary>
        /// Number of duplicates to be created for each ingoing row.
        /// Default is 1 (meaning the incoming row plus one copy).
        /// </summary>
        public int NumberOfDuplicates { get; set; } = 1;

        /// <summary>
        /// A predicate that describe if a will be duplicated or not.
        /// </summary>
        public Predicate<TInput> CanDuplicate { get; set; }

        /// <inheritdoc/>
        public override ISourceBlock<TInput> SourceBlock => TransformBlock;

        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => TransformBlock;

        #endregion

        #region Constructors

        public RowDuplication()
        {
            TypeInfo = new TypeInfo(typeof(TInput)).GatherTypeInfo();
            ObjectCopy = new ObjectCopy<TInput>(TypeInfo);
        }

        /// <param name="numberOfDuplicates">Sets the <see cref="NumberOfDuplicates"/></param>
        public RowDuplication(int numberOfDuplicates) : this()
        {
            this.NumberOfDuplicates = numberOfDuplicates;
        }

        /// <param name="canDuplicate">Sets the <see cref="CanDuplicate"/> predicate</param>
        /// <param name="numberOfDuplicates">Sets the <see cref="NumberOfDuplicates"/></param>
        public RowDuplication(Predicate<TInput> canDuplicate, int numberOfDuplicates) : this(numberOfDuplicates)
        {
            this.CanDuplicate = canDuplicate;
        }

        /// <param name="canDuplicate">Sets the <see cref="CanDuplicate"/> predicate</param>
        public RowDuplication(Predicate<TInput> canDuplicate) : this()
        {
            this.CanDuplicate = canDuplicate;
        }


        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {
            TransformBlock = new TransformManyBlock<TInput, TInput>(DuplicateRow, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        #endregion

        #region Implementation

        TransformManyBlock<TInput, TInput> TransformBlock;
        ObjectCopy<TInput> ObjectCopy;
        TypeInfo TypeInfo;

        private IEnumerable<TInput> DuplicateRow(TInput row)
        {
            NLogStartOnce();
            if (row == null) return null;
            List<TInput> result = new List<TInput>(NumberOfDuplicates);
            result.Add(row);
            LogProgress();
            for (int i = 0; i < NumberOfDuplicates; i++)
            {
                if (CanDuplicate?.Invoke(row) ?? true)
                {
                    TInput copy = ObjectCopy.Clone(row);
                    result.Add(copy);
                    LogProgress();
                }
            }
            return result;
        }

        #endregion
    }

    /// <inheritdoc />
    public class RowDuplication : RowDuplication<ExpandoObject>
    {
        public RowDuplication() : base()
        { }

        public RowDuplication(int numberOfDuplicates) : base(numberOfDuplicates)
        { }

        public RowDuplication(Predicate<ExpandoObject> canDuplicate, int numberOfDuplicates) : base(canDuplicate, numberOfDuplicates)
        { }

        public RowDuplication(Predicate<ExpandoObject> canDuplicate) : base(canDuplicate)
        { }
    }
}
