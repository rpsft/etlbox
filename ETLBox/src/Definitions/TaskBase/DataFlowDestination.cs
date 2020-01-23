using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public abstract class DataFlowDestination<TInput> : DataFlowTask, ITask
    {
        public Action OnCompletion { get; set; }
        public Task Completion { get; protected set; }

        protected ActionBlock<TInput> TargetAction { get; set; }

        public virtual void Wait()
        {
            Completion.Wait();
        }

        protected virtual async Task AwaitCompletion()
        {
            await TargetAction.Completion.ConfigureAwait(false);
            CleanUp();
        }

        protected void SetCompletionTask()
        {
            Completion = AwaitCompletion();
        }

        protected virtual void CleanUp()
        {
            OnCompletion?.Invoke();
            NLogFinish();
        }

    }
}
