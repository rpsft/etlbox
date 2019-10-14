namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowSource<TOutput> : IDataFlowLinkSource<TOutput>
    {
        void ExecuteAsync();
        void LinkTo(IDataFlowLinkTarget<TOutput> target);
    }
}
