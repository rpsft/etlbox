namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowSource<TOutput> : IDataFlowLinkSource<TOutput>
    {
        void PostAll();
        void LinkTo(IDataFlowLinkTarget<TOutput> target);
    }
}
