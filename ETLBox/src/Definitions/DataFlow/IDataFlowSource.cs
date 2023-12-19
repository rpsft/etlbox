namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowSource<out TOutput> : IDataFlowLinkSource<TOutput>, ILinkErrorSource
    {
        Task ExecuteAsync();
        void Execute();
    }
}
