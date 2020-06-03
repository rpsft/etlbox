using ETLBox.ControlFlow;
using System.Dynamic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// This destination if used as a trash.
    /// Redirect all data in this destination which you do not want for further processing.
    /// Every records needs to be transferred to a destination to have a dataflow completed.
    /// </summary>
    /// <typeparam name="TInput">Type of datasoure input.</typeparam>
    public class VoidDestination<TInput> : DataFlowTask, ITask, IDataFlowDestination<TInput>
    {

        /* ITask Interface */
        public override string TaskName => $"Void destination - Ignore data";

        /* Public properties */
        public ITargetBlock<TInput> TargetBlock => _voidDestination?.TargetBlock;

        /* Private stuff */
        CustomDestination<TInput> _voidDestination { get; set; }
        public VoidDestination()
        {
            _voidDestination = new CustomDestination<TInput>(this, row => {; });
        }

        public void Wait() => _voidDestination.Wait();

        public void AddPredecessorCompletion(Task completion) => _voidDestination.AddPredecessorCompletion(completion);

        public Task Completion => _voidDestination.Completion;
    }

    /// <summary>
    /// This destination if used as a trash.
    /// Redirect all data in this destination which you do not want for further processing.
    /// Every records needs to be transferred to a destination to have a dataflow completed.
    /// The non generic implementation works with a dynamic obect as input.
    /// </summary>
    public class VoidDestination : VoidDestination<ExpandoObject>
    {
        public VoidDestination() : base()
        { }
    }
}
