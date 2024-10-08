using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using ETLBox.Primitives;
using JetBrains.Annotations;

namespace ALE.ETLBox.Common.DataFlow
{
    [PublicAPI]
    public abstract class DataFlowDestination<TInput>
        : DataFlowTask,
            IDataFlowDestination<TInput>,
            ILinkErrorSource
    {
        public Action OnCompletion { get; set; }
        public Task Completion { get; protected set; }
        public ITargetBlock<TInput> TargetBlock => TargetAction;

        public virtual void Wait() => Completion.Wait();

        protected ActionBlock<TInput> TargetAction { get; set; }
        protected List<Task> PredecessorCompletions { get; set; } = new();
        protected ErrorHandler ErrorHandler { get; set; } = new();

        public void AddPredecessorCompletion(Task completion)
        {
            PredecessorCompletions.Add(completion);
            completion.ContinueWith(_ => CheckCompleteAction());
        }

        public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target) =>
            ErrorHandler.LinkErrorTo(target, TargetAction.Completion);

        protected void CheckCompleteAction()
        {
            Task.WhenAll(PredecessorCompletions)
                .ContinueWith(t =>
                {
                    if (TargetBlock.Completion.IsCompleted)
                    {
                        return;
                    }

                    if (t.IsFaulted)
                        TargetBlock.Fault(t.Exception!.InnerException!);
                    else
                        TargetBlock.Complete();
                });
        }

        protected void SetCompletionTask() => Completion = AwaitCompletion();

        protected virtual async Task AwaitCompletion()
        {
            try
            {
                await TargetAction.Completion.ConfigureAwait(false);
            }
            catch (AggregateException aggregateException)
            {
                throw aggregateException.InnerException!;
            }
            finally
            {
                CleanUp();
            }
        }

        protected virtual void CleanUp()
        {
            OnCompletion?.Invoke();
            LogFinish();
        }
    }
}
