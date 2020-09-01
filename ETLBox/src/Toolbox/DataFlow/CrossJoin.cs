using ETLBox.ControlFlow;
using ETLBox.DataFlow.Connectors;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// Will cross join data from the two inputs into one output. The input for the first table will be loaded into memory before the actual
    /// join can start. After this, every incoming row will be joined with every row of the InMemory-Table by the function CrossJoinFunc.
    /// The InMemory target should always be the target of the smaller amount of data to reduce memory consumption and processing time.
    /// </summary>
    /// <typeparam name="TInput1">Type of data for in memory input block.</typeparam>
    /// <typeparam name="TInput2">Type of data for processing input block.</typeparam>
    /// <typeparam name="TOutput">Type of output data.</typeparam>
    public class CrossJoin<TInput1, TInput2, TOutput> : DataFlowSource<TOutput>, IDataFlowTransformation<TOutput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Cross join data";
        /// <summary>
        /// The in-memory target of the CrossJoin. This will block processing until all data is received that is designated for this target.
        /// Always have the smaller amount of data flown into this target.
        /// </summary>
        public InMemoryDestination<TInput1> InMemoryTarget { get; set; }
        /// <summary>
        /// Every row that the PassingTarget receives is joined with all data from the <see cref="InMemoryData"/>.
        /// </summary>
        public ActionJoinTarget<TInput2> PassingTarget { get; set; }
        /// <summary>
        /// The cross join function that describes how records from the both target can be joined.
        /// </summary>
        public Func<TInput1, TInput2, TOutput> CrossJoinFunc { get; set; }
        /// <inheritdoc/>
        public override ISourceBlock<TOutput> SourceBlock => this.Buffer;

        #endregion

        #region Join Targets

        public class InMemoryDestination<TInput> : DataFlowJoinTarget<TInput>
        {
            public override ITargetBlock<TInput> TargetBlock => InMemoryTarget.TargetBlock;
            public MemoryDestination<TInput> InMemoryTarget { get; set; }

            public InMemoryDestination(DataFlowComponent parent)
            {
                InMemoryTarget = new MemoryDestination<TInput>();
                CreateLinkInInternalFlow(parent);
            }

            protected override void InternalInitBufferObjects()
            {
                InMemoryTarget.CopyLogTaskProperties(Parent);
                InMemoryTarget.MaxBufferSize = -1;
                InMemoryTarget.InitBufferObjects();
            }

            protected override void CleanUpOnSuccess() { }

            protected override void CleanUpOnFaulted(Exception e) { }
        }

        #endregion

        #region Constructors

        public CrossJoin()
        {
            InMemoryTarget = new InMemoryDestination<TInput1>(this);
            PassingTarget = new ActionJoinTarget<TInput2>(this, CrossJoinData);
        }

        /// <param name="crossJoinFunc">Sets the <see cref="CrossJoinFunc"/></param>
        public CrossJoin(Func<TInput1, TInput2, TOutput> crossJoinFunc) : this()
        {
            CrossJoinFunc = crossJoinFunc;
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
            InMemoryTarget.CompleteBufferOnPredecessorCompletion();
            PassingTarget.CompleteBufferOnPredecessorCompletion();
            Task.WaitAll(InMemoryTarget.Completion, PassingTarget.Completion);
            Buffer.Complete();

        }

        internal override void FaultBufferOnPredecessorCompletion(Exception e)
        {
            InMemoryTarget.FaultBufferOnPredecessorCompletion(e);
            PassingTarget.FaultBufferOnPredecessorCompletion(e);
            ((IDataflowBlock)Buffer).Fault(e);
        }

        #endregion

        #region Implementation

        bool WasInMemoryTableLoaded;
        IEnumerable<TInput1> InMemoryData => InMemoryTarget.InMemoryTarget.Data;

        private void CrossJoinData(TInput2 passingRow)
        {
            NLogStartOnce();
            if (!WasInMemoryTableLoaded)
            {
                InMemoryTarget.Completion.Wait();
                WasInMemoryTableLoaded = true;
            }
            foreach (TInput1 inMemoryRow in InMemoryData)
            {
                try
                {
                    if (inMemoryRow != null && passingRow != null)
                    {
                        TOutput result = CrossJoinFunc.Invoke(inMemoryRow, passingRow);
                        if (result != null)
                        {
                            if (!Buffer.SendAsync(result).Result)
                                throw new ETLBoxException("Buffer already completed or faulted!", this.Exception);
                            LogProgress();
                        }
                    }
                }
                catch (ETLBoxException) { throw; }
                catch (Exception e)
                {
                    ThrowOrRedirectError(e, string.Concat(ErrorSource.ConvertErrorData<TInput1>(inMemoryRow), "  |--| ",
                        ErrorSource.ConvertErrorData<TInput2>(passingRow)));
                    LogProgress();
                }
            }
        }

        #endregion
    }

    /// <inheritdoc/>
    public class CrossJoin<TInput> : CrossJoin<TInput, TInput, TInput>
    {
        public CrossJoin() : base()
        { }

        public CrossJoin(Func<TInput, TInput, TInput> crossJoinFunc) : base(crossJoinFunc)
        { }
    }

    /// <inheritdoc/>
    public class CrossJoin : CrossJoin<ExpandoObject, ExpandoObject, ExpandoObject>
    {
        public CrossJoin() : base()
        { }

        public CrossJoin(Func<ExpandoObject, ExpandoObject, ExpandoObject> crossJoinFunc) : base(crossJoinFunc)
        { }
    }
}

