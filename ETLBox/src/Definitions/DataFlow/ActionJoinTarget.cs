using System;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// A target block that serves as a destination for components that can have multiple inputs.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data</typeparam>
    public sealed class ActionJoinTarget<TInput> : DataFlowJoinTarget<TInput>
    {
        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => JoinAction;

        DataFlowComponent Parent;
        public ActionJoinTarget(DataFlowComponent parent, Action<TInput> action)
        {
            Action = action;
            Parent = parent;
            CreateLinkInInternalFlow(parent);
        }

        ActionBlock<TInput> JoinAction;
        Action<TInput> Action;

        protected override void CheckParameter() { }

        protected override void InitComponent()
        {
            JoinAction = new ActionBlock<TInput>(Action, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize,
                CancellationToken = Parent.CancellationSource.Token
            }) ;
        }

        protected override void CleanUpOnSuccess() { }

        protected override void CleanUpOnFaulted(Exception e) { }
    }
}
