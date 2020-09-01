using ETLBox.ControlFlow;
using ETLBox.DataFlow.Connectors;
using System;
using System.Threading.Tasks.Dataflow;
using CF = ETLBox.ControlFlow;

namespace ETLBox.DataFlow
{
    internal class LinkPredicates
    {
        internal object PredicateKeep { get; set; }
        internal object PredicateVoid { get; set; }

        internal LinkPredicates(object predicateKeep = null, object predicateVoid = null)
        {
            PredicateKeep = predicateKeep;
            PredicateVoid = predicateVoid;
        }
        internal bool HasPredicate => PredicateKeep != null;
        internal bool HasVoidPredicate => PredicateVoid != null;

    }
}
