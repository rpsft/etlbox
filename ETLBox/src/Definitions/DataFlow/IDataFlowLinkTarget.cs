using ETLBox.ControlFlow;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public interface IDataFlowLinkTarget<TInput> : ITask
    {
        ITargetBlock<TInput> TargetBlock { get; }
        void AddPredecessorCompletion(Task completion);
    }
}
