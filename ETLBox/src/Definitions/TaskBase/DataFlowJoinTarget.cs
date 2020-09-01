using ETLBox.ControlFlow;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    /// <summary>
    ///  A target block base implementation
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data</typeparam>
    public abstract class DataFlowJoinTarget<TInput> : DataFlowComponent, IDataFlowDestination<TInput>
    {
        /// <summary>
        /// TargetBlock from the underlying TPL.Dataflow which is used as output buffer for the component.
        /// </summary>
        public virtual ITargetBlock<TInput> TargetBlock { get; }

        internal override Task BufferCompletion => TargetBlock.Completion;

        internal override void CompleteBufferOnPredecessorCompletion() => TargetBlock.Complete();

        internal override void FaultBufferOnPredecessorCompletion(Exception e) => TargetBlock.Fault(e);
        
        protected void CreateLinkInInternalFlow(DataFlowComponent parent)
        {
            Parent = parent;
            InternalLinkTo<TInput>(parent as IDataFlowDestination);
        }
    }
}
