using System.Collections.Concurrent;
using ALE.ETLBox.src.Definitions.TaskBase;
using ALE.ETLBox.src.Definitions.TaskBase.DataFlow;

namespace ALE.ETLBox.src.Toolbox.DataFlow
{
    /// <summary>
    /// A destination in memory - it will store all you data in a list.
    /// </summary>
    /// <see cref="MemoryDestination"/>
    /// <typeparam name="TInput">Type of data input.</typeparam>
    [PublicAPI]
    public class MemoryDestination<TInput> : DataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName => "Write data into memory";

        public BlockingCollection<TInput> Data { get; set; } = new();

        public MemoryDestination()
        {
            TargetAction = new ActionBlock<TInput>(WriteRecord);
            SetCompletionTask();
        }

        internal MemoryDestination(ITask callingTask)
            : this()
        {
            CopyTaskProperties(callingTask);
        }

        protected void WriteRecord(TInput data)
        {
            Data ??= new BlockingCollection<TInput>();
            if (data == null)
                return;
            Data.Add(data);
            LogProgress();
        }

        protected override void CleanUp()
        {
            Data?.CompleteAdding();
            OnCompletion?.Invoke();
            LogFinish();
        }
    }

    /// <summary>
    /// A destination in memory - it will store all you data in a list.
    /// The MemoryDestination uses a dynamic object as input type. If you need other data types, use the generic CsvDestination instead.
    /// </summary>
    /// <see cref="MemoryDestination{TInput}"/>
    [PublicAPI]
    public sealed class MemoryDestination : MemoryDestination<ExpandoObject> { }
}
