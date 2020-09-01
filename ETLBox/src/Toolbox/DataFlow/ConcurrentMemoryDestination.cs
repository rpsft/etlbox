using ETLBox.ControlFlow;
using System;
using System.Collections.Concurrent;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// A destination in memory - it will store all data in a BlockingCollection&lt;T&gt;
    /// The BlockingCollection&lt;T&gt; allows you to access the data concurrently while rows are still written into the target.
    /// If you don't need to work with your data before the flow finishes, you can use
    /// the <see cref="MemoryDestination"/> which uses a regular List&lt;T&gt;.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    public class ConcurrentMemoryDestination<TInput> : DataFlowDestination<TInput>
    {
        #region Public properties

        public override string TaskName => $"Write data into a blocking collection.";
        public BlockingCollection<TInput> Data { get; set; } = new BlockingCollection<TInput>();

        #endregion

        #region Constructors

        public ConcurrentMemoryDestination()
        {

        }

        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {
            TargetAction = new ActionBlock<TInput>(WriteRecord, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize,
                MaxDegreeOfParallelism = 1
            });
        }

        protected override void CleanUpOnSuccess()
        {
            Data?.CompleteAdding();
            NLogFinishOnce();
        }
        protected override void CleanUpOnFaulted(Exception e)
        {
            Data?.CompleteAdding();
        }

        #endregion

        #region Implementation

        protected void WriteRecord(TInput row)
        {
            NLogStartOnce();
            if (Data == null) Data = new BlockingCollection<TInput>();
            if (row == null) return;
            Data.Add(row);
            LogProgress();
        }

        #endregion
    }

    /// <inheritdoc/>
    public class ConcurrentMemoryDestination : ConcurrentMemoryDestination<ExpandoObject>
    {
        public ConcurrentMemoryDestination() : base() { }
    }
}
