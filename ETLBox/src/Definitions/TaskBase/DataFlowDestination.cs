using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public abstract class DataFlowDestination<TInput> : DataFlowTask, ITask, IDataFlowDestination<TInput>
    {
        public Action OnCompletion { get; set; }
        public Task Completion { get; protected set; }
        public ITargetBlock<TInput> TargetBlock => TargetAction;

        protected ActionBlock<TInput> TargetAction { get; set; }
        protected List<Task> PredecessorCompletions { get; set; } = new List<Task>();

        public virtual void Wait() => Completion.Wait();

        public void AddPredecessorCompletion(Task completion)
        {
            PredecessorCompletions.Add(completion);
            completion.ContinueWith(t => CheckCompleteAction());
        }

        protected void CheckCompleteAction()
        {
            Task.WhenAll(PredecessorCompletions).ContinueWith(t =>
            {
                if (!TargetBlock.Completion.IsCompleted)
                {
                    if (t.IsFaulted) TargetBlock.Fault(t.Exception.InnerException);
                    else TargetBlock.Complete();
                }
            });
        }

        protected void SetCompletionTask() => Completion = AwaitCompletion();

        protected virtual async Task AwaitCompletion()
        {
            await TargetAction.Completion.ConfigureAwait(false);
            CleanUp();
        }

        protected virtual void CleanUp()
        {
            OnCompletion?.Invoke();
            NLogFinish();
        }

    }
}
