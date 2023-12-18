using ALE.ETLBox.DataFlow;

namespace ALE.ETLBox.src.Definitions.DataFlow
{
    public interface IDataFlowSource<TOutput> : IDataFlowLinkSource<TOutput>, ILinkErrorSource
    {
        Task ExecuteAsync();
        void Execute();
    }
}
