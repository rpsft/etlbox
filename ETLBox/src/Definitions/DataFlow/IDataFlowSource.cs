namespace ALE.ETLBox.src.Definitions.DataFlow
{
    public interface IDataFlowSource<TOutput> : IDataFlowLinkSource<TOutput>
    {
        Task ExecuteAsync();
        void Execute();

        void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target);
    }
}
