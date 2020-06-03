using ETLBox.ControlFlow;
using ETLBox.DataFlow.Connectors;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;


namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// Will cross join data from the two inputs into one output. The input for the first table will be loaded into memory before the actual
    /// join can start. After this, every incoming row will be joined with every row of the InMemory-Table by the given function CrossJoinFunc.
    /// The InMemory target should always have the smaller amount of data to reduce memory consumption and processing time.
    /// </summary>
    /// <typeparam name="TInput1">Type of data for in memory input block.</typeparam>
    /// <typeparam name="TInput2">Type of data for processing input block.</typeparam>
    /// <typeparam name="TOutput">Type of output data.</typeparam>
    public class CrossJoin<TInput1, TInput2, TOutput> : DataFlowSource<TOutput>, ITask, IDataFlowLinkSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName { get; set; } = "Cross join data";
        public IEnumerable<TInput1> InMemoryData => InMemoryTarget.Data;
        public MemoryDestination<TInput1> InMemoryTarget { get; set; }
        public CustomDestination<TInput2> PassingTarget { get; set; }
        public Func<TInput1, TInput2, TOutput> CrossJoinFunc { get; set; }

        private bool WasInMemoryTableLoaded { get; set; }

        public override void Execute()
        {
            throw new InvalidOperationException("Execute is not supported on CrossJoins! A crossjoin will continue execution" +
            "when the predecessing dataflow components are completed");
        }

        public CrossJoin()
        {
            InMemoryTarget = new MemoryDestination<TInput1>(this);
            PassingTarget = new CustomDestination<TInput2>(this, CrossJoinData);
            PassingTarget.OnCompletion = () => Buffer.Complete();
        }

        public CrossJoin(Func<TInput1, TInput2, TOutput> crossJoinFunc) : this()
        {
            CrossJoinFunc = crossJoinFunc;
        }

        private void CrossJoinData(TInput2 passingRow)
        {
            if (!WasInMemoryTableLoaded)
            {
                InMemoryTarget.Wait();
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
                            Buffer.SendAsync(result).Wait();
                            LogProgress();
                        }
                    }
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    ErrorHandler.Send(e, string.Concat(ErrorHandler.ConvertErrorData<TInput1>(inMemoryRow), "  |--| ",
                        ErrorHandler.ConvertErrorData<TInput2>(passingRow)));
                }
            }

        }
    }

    /// <summary>
    /// Will cross join data from the two inputs into one output. The input for the first table will be loaded into memory before the actual
    /// join can start. After this, every incoming row will be joined with every row of the InMemory-Table by the given function CrossJoinFunc.
    /// The InMemory target should always have the smaller amount of data to reduce memory consumption and processing time.
    /// </summary>
    /// <typeparam name="TInput">Type of data for both inputs and output.</typeparam>
    public class CrossJoin<TInput> : CrossJoin<TInput, TInput, TInput>
    {
        public CrossJoin() : base()
        { }

        public CrossJoin(Func<TInput, TInput, TInput> crossJoinFunc) : base(crossJoinFunc)
        { }
    }

    /// <summary>
    /// Will cross join data from the two inputs into one output. The input for the first table will be loaded into memory before the actual
    /// join can start. After this, every incoming row will be joined with every row of the InMemory-Table by the given function CrossJoinFunc.
    /// The InMemory target should always have the smaller amount of data to reduce memory consumption and processing time.
    /// The non generic implementation deals with a dynamic object for both inputs and output.
    /// </summary>
    public class CrossJoin : CrossJoin<ExpandoObject, ExpandoObject, ExpandoObject>
    {
        public CrossJoin() : base()
        { }

        public CrossJoin(Func<ExpandoObject, ExpandoObject, ExpandoObject> crossJoinFunc) : base(crossJoinFunc)
        { }
    }
}

