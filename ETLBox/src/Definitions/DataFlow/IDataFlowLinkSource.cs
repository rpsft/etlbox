using System;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowLinkSource<TOutput>
    {
        ISourceBlock<TOutput> SourceBlock { get; }
        IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target);
        IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate);

    }
}
