using System.Threading.Tasks;

namespace ETLBox.Primitives
{
    public interface IDataFlowSource<out TOutput> : IDataFlowLinkSource<TOutput>
    {
        Task ExecuteAsync();
        void Execute();

        void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target);
    }
}
