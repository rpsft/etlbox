using ETLBox.ControlFlow;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// A destination in memory - it will store all data in a List&lt;T&gt;
    /// If you need to access the data concurrently while rows are still written into the target,
    /// see the <see cref="ConcurrentMemoryDestination"/>.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    public class MemoryDestination<TInput> : DataFlowDestination<TInput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName => $"Write data into memory";

        /// <summary>
        /// The generic List&lt;T&gt; that will store all rows of incoming data in memory.
        /// </summary>
        public IList<TInput> Data { get; set; } = new List<TInput>();

        #endregion

        #region Constructors

        public MemoryDestination()
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
            NLogFinishOnce();
        }
        protected override void CleanUpOnFaulted(Exception e)
        {
        }

        #endregion

        #region Implementation

        protected void WriteRecord(TInput row)
        {
            NLogStartOnce();
            if (Data == null) Data = new List<TInput>();
            if (row == null) return;
            Data.Add(row);
            LogProgress();
        }

        #endregion
    }

    /// <inheritdoc/>
    public class MemoryDestination : MemoryDestination<ExpandoObject>
    {
        public MemoryDestination() : base() { }
    }
}
