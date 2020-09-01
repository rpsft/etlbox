using ETLBox.ControlFlow;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public abstract class DataFlowDestination<TInput> : DataFlowComponent, IDataFlowDestination<TInput>
    {
        #region Public properties
        /// <inheritdoc/>

        public ITargetBlock<TInput> TargetBlock => TargetAction;

        /// <summary>
        /// Waits for the completion of the component.
        /// </summary>
        public void Wait() => Completion.Wait();

        #endregion

        #region Buffer handling
        protected virtual ActionBlock<TInput> TargetAction { get; set; }

        internal override Task BufferCompletion => TargetBlock.Completion;

        internal override void CompleteBufferOnPredecessorCompletion() => TargetBlock.Complete();

        internal override void FaultBufferOnPredecessorCompletion(Exception e) => TargetBlock.Fault(e);

        #endregion

        /// <summary>
        /// If an error occurs in the component, by default the component will throw an exception and stop execution.
        /// If you use the error linking, any erroneous records will catched and redirected.
        /// </summary>
        /// <param name="target">The target for erroneous rows.</param>
        /// <returns>The linked component.</returns>
        public IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
            => InternalLinkErrorTo(target);
    }
}
