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
    /// Will join data from the two inputs into one output. Make sure both inputs are sorted or in the right order.
    /// Each row from the left join target will be merged with a row from the right join target.
    /// If the amount of ingoing data is unevenly distributed, the last rows will be joined with null values.
    ///
    /// You can define a match condition that let you only merge matching records. This will change the
    /// match behavior a little bit.
    /// By assuming that the intput is sorted, not matching records will be joined with null then. This
    /// can be compared with a left or right join.
    /// </summary>
    /// <typeparam name="TInput1">Type of ingoing data for the left join target.</typeparam>
    /// <typeparam name="TInput2">Type of ingoing data for the right join target.</typeparam>
    /// <typeparam name="TOutput">Type of outgoing data.</typeparam>
    /// <example>
    /// <code>
    /// MergeJoin&lt;MyDataRow1, MyDataRow2, MyDataRow1&gt; join = new MergeJoin&lt;MyDataRow1, MyDataRow2, MyDataRow1&gt;(Func&lt;TInput1, TInput2, TOutput&gt; mergeJoinFunc);
    /// source1.LinkTo(join.LeftJoinTarget);
    /// source2.LinkTo(join.RightJoinTarget);
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
        public ActionJoinTarget<TInput1> LeftJoinTarget { get; set; }
        /// <summary>
        /// The right target of the merge join. Use this to link your source component with.
        /// </summary>
        public ActionJoinTarget<TInput2> RightJoinTarget { get; set; }
        /// <inheritdoc/>
        public override ISourceBlock<TOutput> SourceBlock => this.Buffer;
        /// <summary>
        /// The func that describes how both records from the left and right join target can be joined.
        /// </summary>
        public Func<TInput1, TInput2, TOutput> MergeJoinFunc { get; set; }
        /// <summary>
        /// Define if records should only be joined if the match. Return true if both records do match
        /// and should be joined.
        /// </summary>
        public Func<TInput1, TInput2, bool> BothMatchFunc { get; set; }

        #endregion

        #region Constructors

        public MergeJoin()
        {
            LeftJoinTarget = new ActionJoinTarget<TInput1>(this, LeftJoinData);
            RightJoinTarget = new ActionJoinTarget<TInput2>(this, RightJoinData);
        }

        /// <param name="mergeJoinFunc">Sets the <see cref="MergeJoinFunc"/></param>
        public MergeJoin(Func<TInput1, TInput2, TOutput> mergeJoinFunc) : this()
        {
            MergeJoinFunc = mergeJoinFunc;
        }

        /// <param name="mergeJoinFunc">Sets the <see cref="MergeJoinFunc"/></param>
        /// <param name="bothMatchFunc">Sets the <see cref="BothMatchFunc"/></param>
        public MergeJoin(Func<TInput1, TInput2, TOutput> mergeJoinFunc, Func<TInput1, TInput2, bool> bothMatchFunc) : this(mergeJoinFunc)
        {
            BothMatchFunc = bothMatchFunc;
        }

        #endregion

        #region Implement abstract methods and override default behaviour

        protected override void InternalInitBufferObjects()
        {
            Buffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }


        protected override void CleanUpOnFaulted(Exception e) { }

        protected BufferBlock<TOutput> Buffer { get; set; }
        internal override Task BufferCompletion => SourceBlock.Completion;

        internal override void CompleteBufferOnPredecessorCompletion()
        {
            LeftJoinTarget.CompleteBufferOnPredecessorCompletion();
            RightJoinTarget.CompleteBufferOnPredecessorCompletion();
            Task.WaitAll(LeftJoinTarget.Completion, RightJoinTarget.Completion);
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

        internal override void FaultBufferOnPredecessorCompletion(Exception e)
        {
            LeftJoinTarget.FaultBufferOnPredecessorCompletion(e);
            RightJoinTarget.FaultBufferOnPredecessorCompletion(e);
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
                {
                    TInput1 left = default;
                    TInput2 right = default;
                    if (dataLeft.Count > 0)
                        left = dataLeft.Dequeue();
                    if (dataRight.Count > 0)
                        right = dataRight.Dequeue();
                    CreateOutput(left, right);
                }
            }
        }
        private void LeftJoinData(TInput1 data)
        {
            lock (joinLock)
            {
                if (dataRight.Count >= 1)
                {
                    var right = dataRight.Dequeue();
                    CreateOutput(data, right);
                }
                else
                {
                    dataLeft.Enqueue(data);
                }
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

        private void RightJoinData(TInput2 data)
        {
            lock (joinLock)
            {
                if (dataLeft.Count >= 1)
                {
                    var left = dataLeft.Dequeue();
                    CreateOutput(left, data);
                }
                else
                {
                    dataRight.Enqueue(data);
                }
            }
        }

        #endregion

    }

    /// <summary>
    /// Will join data from the two inputs into one output - on a row by row base. Make sure both inputs are sorted or in the right order.
    /// </summary>
    /// <typeparam name="TInput">Type of data for both inputs and output.</typeparam>
    /// <example>
    /// <code>
    /// MergeJoin&lt;MyDataRow&gt; join = new MergeJoin&lt;MyDataRow&gt;(mergeJoinFunc);
    /// source1.LinkTo(join.Target1);;
    /// source2.LinkTo(join.Target2);;
    /// join.LinkTo(dest);
    /// </code>
    /// </example>
    public class MergeJoin<TInput> : MergeJoin<TInput, TInput, TInput>
    {
        public MergeJoin() : base()
        { }

        public MergeJoin(Func<TInput, TInput, TInput> mergeJoinFunc) : base(mergeJoinFunc)
        { }
    }

    /// <summary>
    /// Will join data from the two inputs into one output - on a row by row base.
    /// Make sure both inputs are sorted or in the right order. The non generic implementation deals with
    /// a dynamic object as input and merged output.
    /// </summary>
    public class MergeJoin : MergeJoin<ExpandoObject, ExpandoObject, ExpandoObject>
    {
        public MergeJoin() : base()
        { }

        public MergeJoin(Func<ExpandoObject, ExpandoObject, ExpandoObject> mergeJoinFunc) : base(mergeJoinFunc)
        { }
    }
}

