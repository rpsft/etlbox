namespace ALE.ETLBox.src.Definitions.DataFlow
{
    public interface IDataFlowBatchDestination<in TInput> : IDataFlowDestination<TInput>
    {
        int BatchSize { get; set; }
    }
}
