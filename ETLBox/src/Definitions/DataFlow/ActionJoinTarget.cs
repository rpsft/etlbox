using ETLBox.ControlFlow;
using NLog.Targets;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// A target block that serves as a destination for components that can have multiple inputs.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data</typeparam>
    public class ActionJoinTarget<TInput> : DataFlowJoinTarget<TInput>
    {
        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock => JoinAction;

        public ActionJoinTarget(DataFlowComponent parent, Action<TInput> action)
        {
            Action = action;
            CreateLinkInInternalFlow(parent);
        }

        ActionBlock<TInput> JoinAction;
        Action<TInput> Action;

        protected override void InternalInitBufferObjects()
        {
            JoinAction = new ActionBlock<TInput>(Action, new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
        }

        protected override void CleanUpOnSuccess() { }

        protected override void CleanUpOnFaulted(Exception e) { }
    }
}
