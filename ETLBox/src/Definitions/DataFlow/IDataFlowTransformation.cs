namespace ETLBox.DataFlow
{
    public interface IDataFlowTransformation<TInput, TOutput> : IDataFlowLinkSource<TOutput>, IDataFlowLinkTarget<TInput>
    {

    }
}
