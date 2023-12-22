using System.Threading.Tasks;

namespace ETLBox.Primitives
{
    public interface IDataFlowSource<out TOutput> : IDataFlowLinkSource<TOutput>, ILinkErrorSource
    {
        Task ExecuteAsync();
        void Execute();
    }
}
