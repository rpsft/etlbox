namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowBatchDestination<in TInput> : IDataFlowDestination<TInput>
    {
        int BatchSize { get; set; }
    }
}
