namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowSource<TOutput> : IDataFlowLinkSource<TOutput>, ILinkErrorSource
    {
        Task ExecuteAsync();
        void Execute();
    }
}
