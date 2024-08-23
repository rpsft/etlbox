using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using ETLBox.Primitives;
using JetBrains.Annotations;

namespace ALE.ETLBox.Common.DataFlow
{
    [PublicAPI]
    public abstract class DataFlowTransformation<TInput, TOutput>
        : DataFlowTask,
            IDataFlowTransformation<TInput, TOutput>,
            ILinkErrorSource
    {
        /// <summary>
        /// Target for the previous component in the data flow.
        /// </summary>
        public virtual ITargetBlock<TInput> TargetBlock { get; }

        /// <summary>
        /// Source for the next component in the data flow.
        /// </summary>
        public virtual ISourceBlock<TOutput> SourceBlock { get; }

        /// <summary>
        /// List of completion Tasks from all preceding components.
        /// </summary>
        protected List<Task> PredecessorCompletions { get; set; } = new();

        /// <summary>
        /// Transformation block component
        /// </summary>
        protected IPropagatorBlock<TInput, TOutput> TransformBlock { get; set; }

        /// <summary>
        /// Error handler
        /// </summary>
        protected ErrorHandler ErrorHandler { get; set; } = new();

        /// <summary>
        /// Link to error target block
        /// </summary>
        /// <param name="target"></param>
        public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target) =>
            ErrorHandler.LinkErrorTo(target, TransformBlock.Completion);

        public void AddPredecessorCompletion(Task completion)
        {
            PredecessorCompletions.Add(completion);
            completion.ContinueWith(_ => CheckCompleteAction());
        }

        protected void CheckCompleteAction()
        {
            Task.WhenAll(PredecessorCompletions)
                .ContinueWith(t =>
                {
                    if (!TargetBlock.Completion.IsCompleted)
                        if (t.IsFaulted)
                            TargetBlock.Fault(t.Exception!.InnerException!);
                        else
                            TargetBlock.Complete();
                });
        }

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target) =>
            new DataFlowLinker<TOutput>(this, SourceBlock).LinkTo(target);

        public IDataFlowLinkSource<TOutput> LinkTo(
            IDataFlowLinkTarget<TOutput> target,
            Predicate<TOutput> predicate
        ) => new DataFlowLinker<TOutput>(this, SourceBlock).LinkTo(target, predicate);

        public IDataFlowLinkSource<TOutput> LinkTo(
            IDataFlowLinkTarget<TOutput> target,
            Predicate<TOutput> rowsToKeep,
            Predicate<TOutput> rowsIntoVoid
        ) =>
            new DataFlowLinker<TOutput>(this, SourceBlock).LinkTo(target, rowsToKeep, rowsIntoVoid);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(
            IDataFlowLinkTarget<TOutput> target
        ) => new DataFlowLinker<TOutput>(this, SourceBlock).LinkTo<TConvert>(target);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(
            IDataFlowLinkTarget<TOutput> target,
            Predicate<TOutput> predicate
        ) => new DataFlowLinker<TOutput>(this, SourceBlock).LinkTo<TConvert>(target, predicate);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(
            IDataFlowLinkTarget<TOutput> target,
            Predicate<TOutput> rowsToKeep,
            Predicate<TOutput> rowsIntoVoid
        ) =>
            new DataFlowLinker<TOutput>(this, SourceBlock).LinkTo<TConvert>(
                target,
                rowsToKeep,
                rowsIntoVoid
            );
    }
}
