using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
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
    public class Sort<TInput> : DataFlowTransformation<TInput, TInput>, ITask, IDataFlowTransformation<TInput, TInput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Sort";

        /* Public Properties */

        public Comparison<TInput> SortFunction
        {
            get { return _sortFunction; }
            set
            {
                _sortFunction = value;
                BlockTransformation = new BlockTransformation<TInput, TInput>(this, SortByFunc);
            }
        }

        public override ISourceBlock<TInput> SourceBlock => BlockTransformation.SourceBlock;
        public override ITargetBlock<TInput> TargetBlock => BlockTransformation.TargetBlock;

        /* Private stuff */
        Comparison<TInput> _sortFunction;
        BlockTransformation<TInput, TInput> BlockTransformation { get; set; }
        public Sort()
        {
            NLogger = NLog.LogManager.GetLogger("ETL");
        }

        public Sort(Comparison<TInput> sortFunction) : this()
        {
            SortFunction = sortFunction;
        }

        public Sort(string name, Comparison<TInput> sortFunction) : this(sortFunction)
        {
            this.TaskName = name;
        }

        List<TInput> SortByFunc(List<TInput> data)
        {
            data.Sort(SortFunction);
            return data;
        }
    }

    /// <summary>
    /// Sort the input with the given sort function. The non generic implementation works with a dyanmic object.
    /// </summary>
    public class Sort : Sort<ExpandoObject>
    {
        public Sort() : base()
        { }

        public Sort(Comparison<ExpandoObject> sortFunction) : base(sortFunction)
        { }

        public Sort(string name, Comparison<ExpandoObject> sortFunction) : base(name, sortFunction)
        { }
    }


}
