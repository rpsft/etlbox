using System.Threading;
using System.Threading.Tasks;

namespace ETLBox.Primitives
{
    public interface IDataFlowSource<out TOutput> : IDataFlowLinkSource<TOutput>
    {
        Task ExecuteAsync(CancellationToken cancellationToken);

        void Execute(CancellationToken cancellationToken);

        void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target);
    }
}
