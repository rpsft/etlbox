using ALE.ETLBox.src.Definitions.TaskBase;

namespace ALE.ETLBox.src.Definitions.DataFlow
{
    public interface IDataFlowLinkTarget<in TInput> : ITask
    {
        ITargetBlock<TInput> TargetBlock { get; }
        void AddPredecessorCompletion(Task completion);
    }
}
