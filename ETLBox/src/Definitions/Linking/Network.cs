using System;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public class Network
    {
        internal static void DoRecursively(DataFlowComponent comp, Action runOnNode, Func<DataFlowComponent, bool> alreadyVisited)
        {
            foreach (DataFlowComponent predecessor in comp.Predecessors)
                if (!alreadyVisited(predecessor))
                    Network.DoRecursively(predecessor, runOnNode, alreadyVisited);

            if (!alreadyVisited(comp))
                runOnNode();

            foreach (DataFlowComponent successor in comp.Successors)
                if (!alreadyVisited(successor))
                    Network.DoRecursively(successor, runOnNode, alreadyVisited);            
        }
    }
}
