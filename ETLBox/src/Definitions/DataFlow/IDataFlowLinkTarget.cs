using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowLinkTarget<TInput>
    {
        ITargetBlock<TInput> TargetBlock { get; }
    }
}
