namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowTransformation<TInput, TOutput> : IDataFlowLinkSource<TOutput>, IDataFlowLinkTarget<TInput>
    {
    }
}
