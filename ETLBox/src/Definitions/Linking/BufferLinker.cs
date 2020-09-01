using ETLBox.ControlFlow;
using ETLBox.DataFlow.Connectors;
using System;
using System.Threading.Tasks.Dataflow;
using CF = ETLBox.ControlFlow;

namespace ETLBox.DataFlow
{
    internal class BufferLinker<T>
    {
        internal LinkPredicates LinkPredicate { get; set; }
        internal Predicate<T> GetPredicateKeep() => LinkPredicate?.PredicateKeep as Predicate<T>;
        internal Predicate<T> GetPredicateVoid() => LinkPredicate?.PredicateVoid as Predicate<T>;
        internal BufferLinker(LinkPredicates linkPredicates = null) {
            LinkPredicate = linkPredicates;
        }

        internal void LinkBlocksWithPredicates(ISourceBlock<T> source, ITargetBlock<T> target)
        {
            if (target == null || source == null)
                throw new ArgumentNullException("Source or target is null - did you call InitBufferObjects() on the linking source and target?");
            if (LinkPredicate?.PredicateKeep != null)
            {
                source.LinkTo<T>(target, GetPredicateKeep());
                if (LinkPredicate.PredicateVoid != null)
                    source.LinkTo<T>(DataflowBlock.NullTarget<T>(), GetPredicateVoid());
            }
            else
                source.LinkTo<T>(target);
        }

    }
}
