using ALE.ETLBox.src.Definitions.DataFlow;
using ALE.ETLBox.src.Definitions.TaskBase;

namespace ALE.ETLBox.src.Toolbox.DataFlow
{
    /// <summary>
    /// This destination if used as a trash.
    /// Redirect all data in this destination which you do not want for further processing.
    /// Every records needs to be transferred to a destination to have a dataflow completed.
    /// </summary>
    /// <typeparam name="TInput">Type of datasoure input.</typeparam>
    [PublicAPI]
    public class VoidDestination<TInput> : DataFlowTask, IDataFlowDestination<TInput>
    {
        /* ITask Interface */
        public override string TaskName => "Void destination - Ignore data";

        /* Public properties */
        public ITargetBlock<TInput> TargetBlock => InternalVoidDestination?.TargetBlock;

        /* Private stuff */
        private CustomDestination<TInput> InternalVoidDestination { get; }

        public VoidDestination()
        {
            InternalVoidDestination = new CustomDestination<TInput>(this, _ => { });
        }

        public void Wait() => InternalVoidDestination.Wait();

        public void AddPredecessorCompletion(Task completion) =>
            InternalVoidDestination.AddPredecessorCompletion(completion);

        public Task Completion => InternalVoidDestination.Completion;
    }

    /// <summary>
    /// This destination if used as a trash.
    /// Redirect all data in this destination which you do not want for further processing.
    /// Every records needs to be transferred to a destination to have a dataflow completed.
    /// The non generic implementation works with a dynamic obect as input.
    /// </summary>
    [PublicAPI]
    public class VoidDestination : VoidDestination<ExpandoObject> { }
}
