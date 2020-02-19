using System;
using System.IO;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    public abstract class DataFlowStreamDestination<TInput> : DataFlowDestination<TInput>
    {
        public string FileName { get; set; }

        protected StreamWriter StreamWriter { get; set; }

        protected void InitTargetAction()
        {
            TargetAction = new ActionBlock<TInput>(WriteData);
            SetCompletionTask();
        }

        protected void WriteData(TInput data)
        {
            if (StreamWriter == null)
            {
                StreamWriter = new StreamWriter(FileName);
                InitStream();
            }
            WriteIntoStream(data);
        }

        protected override void CleanUp()
        {
            CloseStream();
            StreamWriter?.Close();
            OnCompletion?.Invoke();
            NLogFinish();
        }

        protected abstract void InitStream();
        protected abstract void WriteIntoStream(TInput data);
        protected abstract void CloseStream();

    }
}
