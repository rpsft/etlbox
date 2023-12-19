namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowTransformation<in TInput, out TOutput>
        : IDataFlowLinkSource<TOutput>,
            IDataFlowLinkTarget<TInput>
    { }
}
