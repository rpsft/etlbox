using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// Sorts the incoming data with by the given comparison function.
    /// This is a blocking transformation - no output will be produced until all input data has arrived in the transformation.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing (and also outgoing) data.</typeparam>
    /// <example>
    /// <code>
    /// Comparison&lt;MyDataRow&gt; comp = new Comparison&lt;MyDataRow&gt;(
    ///     (x, y) => y.Value2 - x.Value2
    /// );
    /// Sort&lt;MyDataRow&gt; block = new Sort&lt;MyDataRow&gt;(comp);
    /// </code>
    /// </example>
    public class Sort<TInput> : DataFlowTransformation<TInput, TInput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = "Sort";

        /// <inheritdoc/>
        public new int MaxBufferSize => -1;
        
        /// <summary>
        /// A System.Comparison used to sort the data.
        /// </summary>
        public Comparison<TInput> SortFunction { get; set; }

        /// <inheritdoc/>
        public override ISourceBlock<TInput> SourceBlock => BlockTransformation.SourceBlock;
        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => BlockTransformation.TargetBlock;

        #endregion

        #region Constructors

        public Sort()
        {
            BlockTransformation = new BlockTransformation<TInput, TInput>();
        }

        /// <param name="sortFunction">Will set the <see cref="SortFunction"/></param>
        public Sort(Comparison<TInput> sortFunction) : this()
        {
            SortFunction = sortFunction;
        }


        #endregion

        #region Implement abstract methods

        protected override void CheckParameter() { }

        protected override void InitComponent()
        {
            BlockTransformation.CopyLogTaskProperties(this);
            BlockTransformation.BlockTransformationFunc = SortByFunc;
            BlockTransformation.CancellationSource = this.CancellationSource;
            BlockTransformation.InitBufferObjects();
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }
        internal override void CompleteBuffer() => BlockTransformation.CompleteBuffer();

        internal override void FaultBuffer(Exception e) => BlockTransformation.FaultBuffer(e);

        public new IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
        {
            var errorSource = InternalLinkErrorTo(target);
            BlockTransformation.ErrorSource = new ErrorSource() { Redirection = this.ErrorSource };
            return errorSource;
        }

        #endregion

        #region Implementation

        BlockTransformation<TInput, TInput> BlockTransformation { get; set; }

        TInput[] SortByFunc(TInput[] data)
        {
            List<TInput> sortedData = new List<TInput>();
            foreach (var row in data)
                sortedData.Add(row);
            sortedData.Sort(SortFunction);
            return sortedData.ToArray();
        }

        #endregion

    }

    /// <inheritdoc/>
    public class Sort : Sort<ExpandoObject>
    {
        public Sort() : base()
        { }

        public Sort(Comparison<ExpandoObject> sortFunction) : base(sortFunction)
        { }
    }


}
