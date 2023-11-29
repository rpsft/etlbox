namespace ETLBox.Primitives
{
    public interface IDataFlowBatchDestination<in TInput> : IDataFlowDestination<TInput>
    {
        int BatchSize { get; set; }
    }
}
