using System;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    public sealed class Network
    {
        internal static void DoRecursively(DataFlowComponent comp, Func<DataFlowComponent, bool> alreadyRun, Action<DataFlowComponent> runOnNode)
        {
            foreach (DataFlowComponent predecessor in comp.Predecessors)
                if (!alreadyRun(predecessor))
                    Network.DoRecursively(predecessor, alreadyRun, runOnNode);

            if (!alreadyRun(comp))
                runOnNode(comp);

            foreach (DataFlowComponent successor in comp.Successors)
                if (!alreadyRun(successor))
                    Network.DoRecursively(successor, alreadyRun, runOnNode);            
        }
    }
}
