using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using ETLBox.Primitives;
using JetBrains.Annotations;

namespace ALE.ETLBox.Common.DataFlow
{
    [PublicAPI]
    public abstract class DataFlowSource<TOutput> : DataFlowTask
    {
        public ISourceBlock<TOutput> SourceBlock => Buffer;
        protected BufferBlock<TOutput> Buffer { get; set; } = new();

        protected ErrorHandler ErrorHandler { get; set; } = new();

        public abstract void Execute(CancellationToken cancellationToken);

        public void Execute()
            => Execute(CancellationToken.None);

        public Task ExecuteAsync(CancellationToken cancellationToken)
        {
            return Task.Factory.StartNew(() => Execute(cancellationToken));
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

        public void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target) =>
            ErrorHandler.LinkErrorTo(target, SourceBlock.Completion);
    }
}
