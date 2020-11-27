using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// Will join data from the two inputs into one output. Make sure both inputs are sorted or in the right order.
    /// Each row from the left join target will be merged with a row from the right join target.
    /// If the amount of ingoing data is unevenly distributed, the last rows will be joined with null values.
    ///
    /// You can define a match condition that let you only merge matching records. This will change the
    /// match behavior a little bit.
    /// By assuming that the input is sorted, not matching records will be joined with null then. This
    /// can be compared with a left or right join.
    /// </summary>
    /// <typeparam name="TInput1">Type of ingoing data for the left join target.</typeparam>
    /// <typeparam name="TInput2">Type of ingoing data for the right join target.</typeparam>
    /// <typeparam name="TOutput">Type of outgoing data.</typeparam>
    /// <example>
    /// <code>
    /// MergeJoin&lt;InputType1, InputType2, OutputType&gt; join = new MergeJoin&lt;InputType1, InputType2, OutputType&gt;();
    /// join.MergeJoinFunc =  (leftRow, rightRow) => {
    ///     return new OutputType()
    ///     {
    ///         Result = leftRow.Value 1 + rightRow.Value2
    ///     };
    /// });
    /// source1.LinkTo(join.LeftInput);
    /// source2.LinkTo(join.RightInput);
    /// join.LinkTo(dest);
    /// </code>
    /// </example>
    public class MergeJoin<TInput1, TInput2, TOutput> : DataFlowSource<TOutput>, IDataFlowTransformation<TOutput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Merge and join data";

        /// <summary>
        /// The left target of the merge join. Use this to link your source component with.
        /// </summary>
        public ActionJoinTarget<TInput1> LeftInput { get; set; }

        /// <summary>
        /// The right target of the merge join. Use this to link your source component with.
        /// </summary>
        public ActionJoinTarget<TInput2> RightInput { get; set; }

        /// <inheritdoc/>
        public override ISourceBlock<TOutput> SourceBlock => this.Buffer;

        /// <summary>
        /// The func that describes how both records from the left and right join target can be joined.
        /// </summary>
        public Func<TInput1, TInput2, TOutput> MergeJoinFunc { get; set; }

        /// <summary>
        /// If the ComparisonFunc is defined, records are compared regarding their sort order and only joined if they match. 
        /// Return 0 if both records match and should be joined. 
        /// Return a value little than 0 if the record of the left input is in the sort order before the record of the right input.
        /// Return a value greater than 0 if the record for the right input is in the order before the record from the left input.
        /// </summary>
        /// <remarks>Make sure that both inputs are sorted, and the comparison func take the sort order into account.</remarks>
        public Func<TInput1, TInput2, int> ComparisonFunc { get; set; }

        #endregion

        #region Constructors

        public MergeJoin()
        {
            LeftInput = new ActionJoinTarget<TInput1>(this, LeftJoinData);
            RightInput = new ActionJoinTarget<TInput2>(this, RightJoinData);
        }

        /// <param name="mergeJoinFunc">Sets the <see cref="MergeJoinFunc"/></param>
        public MergeJoin(Func<TInput1, TInput2, TOutput> mergeJoinFunc) : this()
        {
            MergeJoinFunc = mergeJoinFunc;
        }

        /// <param name="mergeJoinFunc">Sets the <see cref="MergeJoinFunc"/></param>
        /// <param name="bothMatchFunc">Sets the <see cref="BothMatchFunc"/></param>
        public MergeJoin(Func<TInput1, TInput2, TOutput> mergeJoinFunc, Func<TInput1, TInput2, int> comparisonFunc) : this(mergeJoinFunc)
        {
            ComparisonFunc = comparisonFunc;
        }

        #endregion

        #region Implement abstract methods and override default behavior

        protected override void InternalInitBufferObjects()
        {
            Buffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize,
                CancellationToken = this.CancellationSource.Token
            });
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }


        protected override void CleanUpOnFaulted(Exception e) { }

        protected BufferBlock<TOutput> Buffer { get; set; }
        internal override Task BufferCompletion => SourceBlock.Completion;

        internal override void CompleteBuffer()
        {
            LeftInput.CompleteBuffer();
            RightInput.CompleteBuffer();
            Task.WaitAll(LeftInput.Completion, RightInput.Completion);
            try
            {
                EmptyQueues();
                Buffer.Complete();
            }
            catch (Exception e)
            {
                ((IDataflowBlock)Buffer).Fault(e);
            }
        }

        internal override void FaultBuffer(Exception e)
        {
            LeftInput.FaultBuffer(e);
            RightInput.FaultBuffer(e);
            ((IDataflowBlock)Buffer).Fault(e);
        }

        #endregion

        #region Implementation

        private readonly object joinLock = new object();

        private Queue<TInput1> dataLeft = new Queue<TInput1>();
        private Queue<TInput2> dataRight = new Queue<TInput2>();
        private TOutput joinOutput = default;

        private void EmptyQueues()
        {
            lock (joinLock)
            {
                while (dataLeft.Count > 0 || dataRight.Count > 0)
                    CompareOrJustMerge();
            }
        }
        private void LeftJoinData(TInput1 data)
        {
            lock (joinLock)
            {
                dataLeft.Enqueue(data);
                if (dataRight.Count >= 1)
                    CompareOrJustMerge();
            }
        }

        private void RightJoinData(TInput2 data)
        {
            lock (joinLock)
            {
                dataRight.Enqueue(data);
                if (dataLeft.Count >= 1)
                    CompareOrJustMerge();
            }
        }

        private void CompareOrJustMerge()
        {
            if (ComparisonFunc == null)
                AlwaysMergeBoth();
            else
                MergeByComparison();
        }

        private void AlwaysMergeBoth()
        {
            var left = dataLeft.Count > 0 ? dataLeft.Dequeue() : default;
            var right = dataRight.Count > 0 ? dataRight.Dequeue() : default;
            CreateOutput(left, right);
        }

        private void MergeByComparison()
        {
            var left = dataLeft.Count > 0 ? dataLeft.Peek() : default;
            var right = dataRight.Count > 0 ? dataRight.Peek() : default; ;
            if (right == null)
                CreateOutput(dataLeft.Dequeue(), right);
            else if (left == null)
                CreateOutput(left, dataRight.Dequeue());
            else
            {
                int comp = ComparisonFunc.Invoke(left, right);
                if (comp == 0)
                    CreateOutput(dataLeft.Dequeue(), dataRight.Dequeue());
                else if (comp < 0)
                    CreateOutput(dataLeft.Dequeue(), default);
                else if (comp > 0)
                    CreateOutput(default, dataRight.Dequeue());
            }
        }


        private void CreateOutput(TInput1 dataLeft, TInput2 dataRight)
        {
            try
            {
                joinOutput = MergeJoinFunc.Invoke(dataLeft, dataRight);
                if (!Buffer.SendAsync(joinOutput).Result)
                    throw new ETLBoxException("Buffer already completed or faulted!", this.Exception);
            }
            catch (ETLBoxException) { throw; }
            catch (Exception e)
            {
                ThrowOrRedirectError(e, "Left:" + ErrorSource.ConvertErrorData<TInput1>(dataLeft)
                                        + "|Right:" + ErrorSource.ConvertErrorData<TInput2>(dataRight));
            }
        }

        #endregion

    }

    /// <inheritdoc />
    public class MergeJoin<TInput> : MergeJoin<TInput, TInput, TInput>
    {
        public MergeJoin() : base()
        { }

        public MergeJoin(Func<TInput, TInput, TInput> mergeJoinFunc) : base(mergeJoinFunc)
        { }
    }

    /// <inheritdoc />
    public class MergeJoin : MergeJoin<ExpandoObject, ExpandoObject, ExpandoObject>
    {
        public MergeJoin() : base()
        { }

        public MergeJoin(Func<ExpandoObject, ExpandoObject, ExpandoObject> mergeJoinFunc) : base(mergeJoinFunc)
        { }
    }
}

