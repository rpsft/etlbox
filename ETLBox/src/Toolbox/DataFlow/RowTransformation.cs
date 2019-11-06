using System;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Transforms the data row-by-row with the help of the transformation function.
    /// </summary>
    /// <typeparam name="TInput">Type of input data.</typeparam>
    /// <typeparam name="TOutput">Type of output data.</typeparam>
    /// <see cref="RowTransformation"/>
    /// <example>
    /// <code>
    /// RowTransformation&lt;string[], MyDataRow&gt; trans = new RowTransformation&lt;string[], MyDataRow&gt;(
    ///     csvdata => {
    ///       return new MyDataRow() { Value1 = csvdata[0], Value2 = int.Parse(csvdata[1]) };
    /// });
    /// trans.LinkTo(dest);
    /// </code>
    /// </example>
    public class RowTransformation<TInput, TOutput> : DataFlowTask, ITask, IDataFlowTransformation<TInput, TOutput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Dataflow: Row Transformation";

        /* Public Properties */
        public Func<TInput, TOutput> RowTransformationFunc
        {
            get
            {
                return _rowTransformationFunc;
            }

            set
            {
                _rowTransformationFunc = value;
                TransformBlock = new TransformBlock<TInput, TOutput>(row => InvokeRowTransformationFunc(row));
            }
        }
        public Action InitAction { get; set; }
        public bool WasInitialized { get; private set; } = false;

        public ITargetBlock<TInput> TargetBlock => TransformBlock;
        public ISourceBlock<TOutput> SourceBlock => TransformBlock;

        /* Private stuff */
        Func<TInput, TOutput> _rowTransformationFunc;
        internal TransformBlock<TInput, TOutput> TransformBlock { get; set; }
        public RowTransformation()
        {
        }

        public RowTransformation(Func<TInput, TOutput> rowTransformationFunc) : this()
        {
            RowTransformationFunc = rowTransformationFunc;
        }

        public RowTransformation(string name, Func<TInput, TOutput> rowTransformationFunc) : this(rowTransformationFunc)
        {
            this.TaskName = name;
        }

        public RowTransformation(string name, Func<TInput, TOutput> rowTransformationFunc, Action initAction) : this(rowTransformationFunc)
        {
            this.TaskName = name;
            this.InitAction = initAction;
        }

        public RowTransformation(ITask task) : this()
        {
            CopyTaskProperties(task);
        }

        public RowTransformation(ITask task, Func<TInput, TOutput> rowTransformationFunc) : this(rowTransformationFunc)
        {
            CopyTaskProperties(task);
        }

        private void CopyTaskProperties(ITask task)
        {
            this.TaskHash = task.TaskHash;
            this.TaskName = task.TaskName;
            this.TaskType = task.TaskType;
            this.DisableLogging = task.DisableLogging;
        }



        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo(target);

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo(target, predicate);

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo(target, rowsToKeep, rowsIntoVoid);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo<TConvert>(target);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo<TConvert>(target, predicate);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);

        private TOutput InvokeRowTransformationFunc(TInput row)
        {
            if (!WasInitialized)
            {
                InitAction?.Invoke();
                WasInitialized = true;
                if (!DisableLogging)
                    NLogger.Debug(TaskName + " was initialized!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            }
            LogProgress(1);
            return RowTransformationFunc.Invoke(row);
        }

        void LogProgress(int rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (!DisableLogging && HasLoggingThresholdRows && (ProgressCount % LoggingThresholdRows == 0))
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

    }

    /// <summary>
    /// Transforms the data row-by-row with the help of the transformation function.
    /// </summary>
    /// <typeparam name="TInput">Type of input (and output) data.</typeparam>
    /// <see cref="RowTransformation{TInput, TOutput}"/>
    /// <example>
    /// <code>
    /// RowTransformation&lt;MyDataRow&gt; trans = new RowTransformation&lt;MyDataRow&gt;(
    ///     row => {
    ///       row.Value += 1;
    ///       return row;
    /// });
    /// trans.LinkTo(dest);
    /// </code>
    /// </example>
    public class RowTransformation<TInput> : RowTransformation<TInput, TInput>
    {
        public RowTransformation() : base() { }
        public RowTransformation(Func<TInput, TInput> rowTransformationFunc) : base(rowTransformationFunc) { }
        public RowTransformation(string name, Func<TInput, TInput> rowTransformationFunc) : base(name, rowTransformationFunc) { }
        public RowTransformation(string name, Func<TInput, TInput> rowTransformationFunc, Action initAction) : base(name, rowTransformationFunc, initAction) { }
        public RowTransformation(ITask task) : base(task) { }
        public RowTransformation(ITask task, Func<TInput, TInput> rowTransformationFunc) : base(rowTransformationFunc) { }
    }

    /// <summary>
    /// Transforms the data row-by-row with the help of the transformation function.
    /// The non generic RowTransformation accepts a string array as input and returns a string array as output.
    /// If you need other data types, use the generic RowTransformation instead.
    /// </summary>
    /// <see cref="RowTransformation{TInput, TOutput}"/>
    /// <example>
    /// <code>
    /// //Non generic RowTransformation works with string[] as input and output
    /// //use RowTransformation&lt;TInput,TOutput&gt; for generic usage!
    /// RowTransformation trans = new RowTransformation(
    ///     csvdata => {
    ///       return new string[] { csvdata[0],  int.Parse(csvdata[1]) };
    /// });
    /// trans.LinkTo(dest);
    /// </code>
    /// </example>
    public class RowTransformation : RowTransformation<string[]>
    {
        public RowTransformation() : base() { }
        public RowTransformation(Func<string[], string[]> rowTransformationFunc) : base(rowTransformationFunc) { }
        public RowTransformation(string name, Func<string[], string[]> rowTransformationFunc) : base(name, rowTransformationFunc) { }
        public RowTransformation(string name, Func<string[], string[]> rowTransformationFunc, Action initAction) : base(name, rowTransformationFunc, initAction) { }
        public RowTransformation(ITask task) : base(task)
        { }
        public RowTransformation(ITask task, Func<string[], string[]> rowTransformationFunc) : base(rowTransformationFunc)
        { }
    }
}
