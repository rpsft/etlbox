using System.Collections.Concurrent;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// A destination in memory - it will store all you data in a list.
    /// </summary>
    /// <see cref="MemoryDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    public class MemoryDestination<TInput> : DataFlowDestination<TInput>, ITask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName => $"Write data into memory";

        public BlockingCollection<TInput> Data { get; set; } = new BlockingCollection<TInput>();

        public MemoryDestination()
        {
            TargetAction = new ActionBlock<TInput>(WriteRecord);
            SetCompletionTask();
        }

        internal MemoryDestination(ITask callingTask) : this()
        {
            CopyTaskProperties(callingTask);
        }

        protected void WriteRecord(TInput data)
        {
            if (Data == null) Data = new BlockingCollection<TInput>();
            if (data == null) return;
            Data.Add(data);
            LogProgress();
        }

        protected override void CleanUp()
        {
            Data?.CompleteAdding();
            OnCompletion?.Invoke();
            NLogFinish();
        }
    }

    /// <summary>
    /// A destination in memory - it will store all you data in a list.
    /// The MemoryDestination uses a dynamic object as input type. If you need other data types, use the generic CsvDestination instead.
    /// </summary>
    /// <see cref="MemoryDestination{TInput}"/>
    public class MemoryDestination : MemoryDestination<ExpandoObject>
    {
        public MemoryDestination() : base() { }
    }
}
