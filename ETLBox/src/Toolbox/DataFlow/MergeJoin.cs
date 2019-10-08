using System;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow {
    /// <summary>
    /// Will join data from the two inputs into one output - on a row by row base. Make sure both inputs are sorted or in the right order.
    /// </summary>
    /// <typeparam name="TInput1">Type of data for input block one.</typeparam>
    /// <typeparam name="TInput2">Type of data for input block two.</typeparam>
    /// <typeparam name="TOutput">Type of output data.</typeparam>
    /// <example>
    /// <code>
    /// MergeJoin&lt;MyDataRow1, MyDataRow2, MyDataRow1&gt; join = new MergeJoin&lt;MyDataRow1, MyDataRow2, MyDataRow1&gt;(Func&lt;TInput1, TInput2, TOutput&gt; mergeJoinFunc);
    /// source1.LinkTo(join.Target1);;
    /// source2.LinkTo(join.Target2);;
    /// join.LinkTo(dest);
    /// </code>
    /// </example>
    public class MergeJoin<TInput1, TInput2, TOutput> : DataFlowTask, ITask, IDataFlowLinkSource<TOutput> {
        private Func<TInput1, TInput2, TOutput> _mergeJoinFunc;

        /* ITask Interface */
        public override string TaskName { get; set; } = "Dataflow: Mergejoin";

        /* Public Properties */
        public MergeJoinTarget<TInput1> Target1 { get; set; }
        public MergeJoinTarget<TInput2> Target2 { get; set; }
        public ISourceBlock<TOutput> SourceBlock => Transformation.SourceBlock;

        public Func<TInput1, TInput2, TOutput> MergeJoinFunc {
            get { return _mergeJoinFunc; }
            set {
                _mergeJoinFunc = value;
                Transformation.RowTransformationFunc = new Func<Tuple<TInput1, TInput2>, TOutput>(tuple => _mergeJoinFunc.Invoke(tuple.Item1, tuple.Item2));
                JoinBlock.LinkTo(Transformation.TargetBlock, new DataflowLinkOptions { PropagateCompletion = true });
            }
        }

        /* Private stuff */
        internal BufferBlock<TInput1> Buffer1 { get; set; }
        internal BufferBlock<TInput1> Buffer2 { get; set; }
        internal JoinBlock<TInput1, TInput2> JoinBlock { get; set; }
        internal RowTransformation<Tuple<TInput1, TInput2>, TOutput> Transformation { get; set; }

        public MergeJoin() {
            Transformation = new RowTransformation<Tuple<TInput1, TInput2>, TOutput>(this);
            JoinBlock = new JoinBlock<TInput1, TInput2>();
            Target1 = new MergeJoinTarget<TInput1>(JoinBlock.Target1);
            Target2 = new MergeJoinTarget<TInput2>(JoinBlock.Target2);
        }

        public MergeJoin(Func<TInput1, TInput2, TOutput> mergeJoinFunc) : this() {
            MergeJoinFunc = mergeJoinFunc;
        }

        public MergeJoin(string name) : this() {
            this.TaskName = name;
        }

        public void LinkTo(IDataFlowLinkTarget<TOutput> target) {
            Transformation.LinkTo(target);
            if (!DisableLogging)
                NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        public void LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate) {
            Transformation.LinkTo(target, predicate);
            if (!DisableLogging)
                NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

    }

    public class MergeJoinTarget<TInput> : IDataFlowDestination<TInput>{
        public ITargetBlock<TInput> TargetBlock { get; set; }

        public void Wait() {
            TargetBlock.Completion.Wait();
        }
        public MergeJoinTarget(ITargetBlock<TInput> joinTarget) {
            TargetBlock = joinTarget;
        }
    }

    /// <summary>
    /// Will join data from the two inputs into one output - on a row by row base.
    /// Make sure both inputs are sorted or in the right order. The non generic implementation deals with
    /// string array as inputs and merged output.
    /// </summary>
    public class MergeJoin : MergeJoin<string[], string[], string[]>
    {
        public MergeJoin() : base()
        { }

        public MergeJoin(Func<string[], string[], string[]> mergeJoinFunc) : base(mergeJoinFunc)
        { }

        public MergeJoin(string name) : base(name)
        { }
    }
}

