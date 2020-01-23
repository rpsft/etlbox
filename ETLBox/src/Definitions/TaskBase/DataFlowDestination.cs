using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public abstract class DataFlowDestination<TInput> : DataFlowTask, ITask
    {
        internal virtual void CleanUp()
        {
            OnCompletion?.Invoke();
            NLogFinish();
        }

    }
}
