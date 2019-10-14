using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowLinkSource<TOutput>
    {
        ISourceBlock<TOutput> SourceBlock { get; }
    }
}
