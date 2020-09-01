using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using NLog.Targets;
using System;
using System.Linq.Expressions;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// The base implementation for a source component.
    /// </summary>
    /// <typeparam name="TOutput">Type of outgoing data</typeparam>
    public abstract class DataFlowSource<TOutput> : DataFlowComponent, IDataFlowSource<TOutput>
    {
        /// <inheritdoc/>
        public abstract ISourceBlock<TOutput> SourceBlock { get; }

        internal override void LinkBuffers(DataFlowComponent successor, LinkPredicates linkPredicates)
        {
            var s = successor as IDataFlowDestination<TOutput>;
            var linker = new BufferLinker<TOutput>(linkPredicates);
            linker.LinkBlocksWithPredicates(SourceBlock, s.TargetBlock);
        }

        /// <inheritdoc/>
        public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target)
            => InternalLinkTo<TOutput>(target);

        /// <inheritdoc/>
        public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep)
           => InternalLinkTo<TOutput>(target, rowsToKeep);

        /// <inheritdoc/>
        public IDataFlowSource<TOutput> LinkTo(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => InternalLinkTo<TOutput>(target, rowsToKeep, rowsIntoVoid);

        /// <inheritdoc/>
        public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target)
             => InternalLinkTo<TConvert>(target);

        /// <inheritdoc/>
        public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep)
            => InternalLinkTo<TConvert>(target, rowsToKeep);

        /// <inheritdoc/>
        public IDataFlowSource<TConvert> LinkTo<TConvert>(IDataFlowDestination<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => InternalLinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);

        /// <inheritdoc/>
        public IDataFlowSource<ETLBoxError> LinkErrorTo(IDataFlowDestination<ETLBoxError> target)
            => InternalLinkErrorTo(target);
    }
}
