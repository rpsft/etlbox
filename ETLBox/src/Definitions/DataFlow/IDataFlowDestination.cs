namespace ALE.ETLBox.src.Definitions.DataFlow
{
    public interface IDataFlowDestination<in TInput> : IDataFlowLinkTarget<TInput>
    {
        void Wait();
        Task Completion { get; }
    }
}
