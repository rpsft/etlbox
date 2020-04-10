using ALE.ETLBox.Helper;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow
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
        public override ISourceBlock<TInput> SourceBlock => OutputBuffer;
        public override ITargetBlock<TInput> TargetBlock => InputBuffer;


        /* Private stuff */
        BufferBlock<TInput> OutputBuffer { get; set; }
        ActionBlock<TInput> InputBuffer { get; set; }
        bool WasInitialized { get; set; }
        ObjectCopy<TInput> ObjectCopy { get; set; }
        TypeInfo TypeInfo { get; set; }

        public RowDuplication()
        {
            TypeInfo = new TypeInfo(typeof(TInput));
            ObjectCopy = new ObjectCopy<TInput>(TypeInfo);
            OutputBuffer = new BufferBlock<TInput>();
            InputBuffer = new ActionBlock<TInput>(DuplicateRow);
            InputBuffer.Completion.ContinueWith(FinishFlow);
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

        public void InitFlow()
        {
            if (!WasInitialized)
            {
                NLogStart();
                WasInitialized = true;
            }
        }

        private void DuplicateRow(TInput row)
        {
            if (row == null) return;
            OutputBuffer.SendAsync(row).Wait();
            LogProgress();
            for (int i = 0; i < NumberOfDuplicates; i++)
            {
                if (CanDuplicate?.Invoke(row) ?? true)
                {
                    TInput copy = ObjectCopy.Clone(row);
                    OutputBuffer.SendAsync(copy).Wait();
                    LogProgress();
                }
            }
        }

        private void FinishFlow(Task t)
        {
            CompleteOrFaultBuffer(t, OutputBuffer);
            NLogFinish();
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
