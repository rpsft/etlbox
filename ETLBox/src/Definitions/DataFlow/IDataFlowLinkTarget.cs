using System;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public interface IDataFlowLinkTarget<TInput> : ITask
    {
        ITargetBlock<TInput> TargetBlock { get; }
      }
}
