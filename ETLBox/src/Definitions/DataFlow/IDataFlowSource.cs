using System.Threading.Tasks;

namespace ETLBox.DataFlow
{
    public interface IDataFlowSource<TOutput> : IDataFlowLinkSource<TOutput>
    {
        Task ExecuteAsync();
        void Execute();

        void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target);
    }
}
