namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowLinkTarget<in TInput> : ITask
    {
        ITargetBlock<TInput> TargetBlock { get; }
        void AddPredecessorCompletion(Task completion);
    }
}
