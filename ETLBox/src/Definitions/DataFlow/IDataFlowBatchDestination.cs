namespace ETLBox.DataFlow
{
    public interface IDataFlowBatchDestination<TInput> : IDataFlowDestination<TInput>
    {
        int BatchSize { get; set; }
    }
}
