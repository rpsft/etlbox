namespace ALE.ETLBox.src.Definitions.DataFlow
{
    public interface IDataFlowTransformation<in TInput, out TOutput>
        : IDataFlowLinkSource<TOutput>,
            IDataFlowLinkTarget<TInput>
    { }
}
