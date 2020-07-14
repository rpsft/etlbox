using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// Creates one or more duplicates of your incoming rows.
    /// </summary>
    /// <typeparam name="TInput">Type of data input</typeparam>
    public class RowDuplication<TInput> : DataFlowTransformation<TInput, TInput>, ITask, IDataFlowTransformation<TInput, TInput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = $"Duplicate rows.";

        /* Public Properties */
        public int NumberOfDuplicates { get; set; } = 1;
        public Predicate<TInput> CanDuplicate { get; set; }
        public override ISourceBlock<TInput> SourceBlock => TransformBlock;
        public override ITargetBlock<TInput> TargetBlock => TransformBlock;

        /* Private stuff */
        TransformManyBlock<TInput, TInput> TransformBlock { get; set; }
        ObjectCopy<TInput> ObjectCopy { get; set; }
        TypeInfo TypeInfo { get; set; }

        public RowDuplication()
        {
            TypeInfo = new TypeInfo(typeof(TInput)).GatherTypeInfo();
            ObjectCopy = new ObjectCopy<TInput>(TypeInfo);
            InitBufferObjects();
        }

        public RowDuplication(int numberOfDuplicates) : this()
        {
            this.NumberOfDuplicates = numberOfDuplicates;
        }

        public RowDuplication(Predicate<TInput> canDuplicate, int numberOfDuplicates) : this(numberOfDuplicates)
        {
            this.CanDuplicate = canDuplicate;
        }

        public RowDuplication(Predicate<TInput> canDuplicate) : this()
        {
            this.CanDuplicate = canDuplicate;
        }

        protected override void InitBufferObjects()
        {
            TransformBlock = new TransformManyBlock<TInput, TInput>(DuplicateRow, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
        }

        private IEnumerable<TInput> DuplicateRow(TInput row)
        {
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
    }

    /// <summary>
    /// Creates one or more duplicates of your incoming rows.
    /// The non generic implementation works with dynamic object.
    /// </summary>
    /// <see cref="RowDuplication{TInput}"/>
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
