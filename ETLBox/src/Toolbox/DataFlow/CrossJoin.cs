using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Will cross join data from the two inputs into one output. The input for the first table will be kept in while in memory.
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
    public class CrossJoin<TInput1, TInput2, TOutput> : DataFlowSource<TOutput>//DataFlowTask, ITask, IDataFlowLinkSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Cross join data";
        public List<TInput1> SmallTableData { get; set; } = new List<TInput1>();
        public CustomDestination<TInput1> SmallTableDest { get; set; }
        public CustomDestination<TInput2> BigTableDest { get; set; }
        public Func<TInput1, TInput2, TOutput> CrossJoinFunc { get; set; }

        private bool WasSmallTableLoaded { get; set; }

        public override void Execute()
        {
            throw new InvalidOperationException("Execute is not supported on CrossJoins! A crossjoin will continue execution" +
            "when the predecessing dataflow components are completed");
        }
        public CrossJoin()
        {
            SmallTableDest = new CustomDestination<TInput1>(this, FillSmallTableBuffer);
            BigTableDest = new CustomDestination<TInput2>(this, CrossJoinData);
            BigTableDest.OnCompletion = () => Buffer.Complete();
        }

        public CrossJoin(Func<TInput1, TInput2, TOutput> crossJoinFunc) : this()
        {
            CrossJoinFunc = crossJoinFunc;
        }

        private void FillSmallTableBuffer(TInput1 smallTableRow)
        {
            if (SmallTableData == null) SmallTableData = new List<TInput1>();
            SmallTableData.Add(smallTableRow);
        }

        private void CrossJoinData(TInput2 bigTableRow)
        {
            if (!WasSmallTableLoaded)
            {
                SmallTableDest.Wait();
                WasSmallTableLoaded = true;
            }
            foreach (TInput1 smallTableRow in SmallTableData)
            {
                try
                {
                    if (smallTableRow != null && bigTableRow != null)
                    {
                        TOutput result = CrossJoinFunc.Invoke(smallTableRow, bigTableRow);
                        Buffer.SendAsync(result).Wait();
                    }
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    ErrorHandler.Send(e, string.Concat(ErrorHandler.ConvertErrorData<TInput1>(smallTableRow), "  |--| ",
                        ErrorHandler.ConvertErrorData<TInput2>(bigTableRow)));
                }
                LogProgress();
            }

        }
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
    public class CrossJoin<TInput> : CrossJoin<TInput, TInput, TInput>
    {
        public CrossJoin() : base()
        { }

        public CrossJoin(Func<TInput, TInput, TInput> crossJoinFunc) : base(crossJoinFunc)
        { }
    }

    /// <summary>
    /// Will join data from the two inputs into one output - on a row by row base.
    /// Make sure both inputs are sorted or in the right order. The non generic implementation deals with
    /// a dynamic object as input and merged output.
    /// </summary>
    public class CrossJoin : CrossJoin<ExpandoObject, ExpandoObject, ExpandoObject>
    {
        public CrossJoin() : base()
        { }

        public CrossJoin(Func<ExpandoObject, ExpandoObject, ExpandoObject> crossJoinFunc) : base(crossJoinFunc)
        { }
    }
}

