using ALE.ETLBox.DataFlow;
using System;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox
{
    public abstract class DataFlowTransformation<TInput, TOutput> : DataFlowTask, ITask, IDataFlowTransformation<TInput, TOutput>
    {
        public virtual ITargetBlock<TInput> TargetBlock { get; }
        public virtual ISourceBlock<TOutput> SourceBlock { get; }
        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target)
        => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo(target);

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo(target, predicate);

        public IDataFlowLinkSource<TOutput> LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo(target, rowsToKeep, rowsIntoVoid);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo<TConvert>(target);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo<TConvert>(target, predicate);

        public IDataFlowLinkSource<TConvert> LinkTo<TConvert>(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> rowsToKeep, Predicate<TOutput> rowsIntoVoid)
            => (new DataFlowLinker<TOutput>(this, SourceBlock, DisableLogging)).LinkTo<TConvert>(target, rowsToKeep, rowsIntoVoid);
    }
}
