using System.Threading.Tasks;

namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowSource<TOutput> : IDataFlowLinkSource<TOutput>
    {
        Task StartPostAll();
        void LinkTo(IDataFlowLinkTarget<TOutput> target);
    }
}
