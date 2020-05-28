using System;
using System.Dynamic;
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
    public class RowTransformation<TInput, TOutput> : DataFlowTransformation<TInput, TOutput>, ITask, IDataFlowTransformation<TInput, TOutput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Execute row transformation";

        /* Public Properties */
        public Func<TInput, TOutput> TransformationFunc
        {
            get
            {
                return _transformationFunc;
            }

            set
            {
                _transformationFunc = value;
                TransformBlock = new TransformBlock<TInput, TOutput>(
                    row =>
                    {
                        try
                        {
                            return WrapTransformation(row);
                        }
                        catch (Exception e)
                        {
                            if (!ErrorHandler.HasErrorBuffer) throw e;
                            ErrorHandler.Send(e, ErrorHandler.ConvertErrorData<TInput>(row));
                            return default(TOutput);
                        }
                    }
                );
            }
        }
        public Action InitAction { get; set; }
        public bool WasInitialized { get; private set; } = false;

        public override ITargetBlock<TInput> TargetBlock => TransformBlock;
        public override ISourceBlock<TOutput> SourceBlock => TransformBlock;

        /* Private stuff */
        Func<TInput, TOutput> _transformationFunc;
        internal TransformBlock<TInput, TOutput> TransformBlock { get; set; }
        internal ErrorHandler ErrorHandler { get; set; } = new ErrorHandler();

        public RowTransformation()
        {
        }

        public RowTransformation(Func<TInput, TOutput> rowTransformationFunc) : this()
        {
            TransformationFunc = rowTransformationFunc;
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

        internal RowTransformation(ITask task) : this()
        {
            CopyTaskProperties(task);
        }

        internal RowTransformation(ITask task, Func<TInput, TOutput> rowTransformationFunc) : this(rowTransformationFunc)
        {
            CopyTaskProperties(task);
        }

        public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target)
            => ErrorHandler.LinkErrorTo(target, TransformBlock.Completion);


        private TOutput WrapTransformation(TInput row)
        {
            if (!WasInitialized)
            {
                InitAction?.Invoke();
                WasInitialized = true;
                if (!DisableLogging)
                    NLogger.Debug(TaskName + " was initialized!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.Id);
            }
            LogProgress();
            return TransformationFunc.Invoke(row);
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
    }

    /// <summary>
    /// Transforms the data row-by-row with the help of the transformation function.
    /// The non generic RowTransformation accepts a dynamic object as input and returns a dynamic object as output.
    /// If you need other data types, use the generic RowTransformation instead.
    /// </summary>
    /// <see cref="RowTransformation{TInput, TOutput}"/>
    /// <example>
    /// <code>
    /// //Non generic RowTransformation works with dynamic object as input and output
    /// //use RowTransformation&lt;TInput,TOutput&gt; for generic usage!
    /// RowTransformation trans = new RowTransformation(
    ///     csvdata => {
    ///       return new string[] { csvdata[0],  int.Parse(csvdata[1]) };
    /// });
    /// trans.LinkTo(dest);
    /// </code>
    /// </example>
    public class RowTransformation : RowTransformation<ExpandoObject>
    {
        public RowTransformation() : base() { }
        public RowTransformation(Func<ExpandoObject, ExpandoObject> rowTransformationFunc) : base(rowTransformationFunc) { }
        public RowTransformation(string name, Func<ExpandoObject, ExpandoObject> rowTransformationFunc) : base(name, rowTransformationFunc) { }
        public RowTransformation(string name, Func<ExpandoObject, ExpandoObject> rowTransformationFunc, Action initAction) : base(name, rowTransformationFunc, initAction) { }
    }
}
