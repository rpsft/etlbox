using System;
using System.Collections.Generic;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow {
    /// <summary>
    /// Sort the input with the given sort function.
    /// </summary>
    /// <typeparam name="TInput">Type of input data (equal type of output data).</typeparam>
    /// <example>
    /// <code>
    /// Comparison&lt;MyDataRow&gt; comp = new Comparison&lt;MyDataRow&gt;(
    ///     (x, y) => y.Value2 - x.Value2
    /// );
    /// Sort&lt;MyDataRow&gt; block = new Sort&lt;MyDataRow&gt;(comp);
    /// </code>
    /// </example>
    public class Sort<TInput> : DataFlowTask, ITask, IDataFlowLinkTarget<TInput>, IDataFlowLinkSource<TInput> {


        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_SORT";
        public override string TaskName { get; set; } = "Dataflow: Sort";
        public override void Execute() { throw new Exception("Transformations can't be executed directly"); }

        /* Public Properties */

        public Comparison<TInput> SortFunction {
            get { return _sortFunction; }
            set {
                _sortFunction = value;
                BlockTransformation = new BlockTransformation<TInput>(this, SortByFunc);
            }
        }

        public ISourceBlock<TInput> SourceBlock => BlockTransformation.SourceBlock;
        public ITargetBlock<TInput> TargetBlock => BlockTransformation.TargetBlock;

        /* Private stuff */
        Comparison<TInput> _sortFunction;
        BlockTransformation<TInput> BlockTransformation { get; set; }
        NLog.Logger NLogger { get; set; }

        public Sort() {
            NLogger = NLog.LogManager.GetLogger("ETL");
        }

        public Sort(Comparison<TInput> sortFunction) : this() {
            SortFunction = sortFunction;
        }

        public Sort(string name, Comparison<TInput> sortFunction) : this(sortFunction) {
            this.TaskName = name;
        }

        List<TInput> SortByFunc(List<TInput> data) {
            data.Sort(SortFunction);
            return data;
        }

        public void LinkTo(IDataFlowLinkTarget<TInput> target) {
            BlockTransformation.LinkTo(target);
            if (!DisableLogging)
                NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        public void LinkTo(IDataFlowLinkTarget<TInput> target, Predicate<TInput> predicate) {
            BlockTransformation.LinkTo(target, predicate);
            if (!DisableLogging)
                NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }
    }


}
