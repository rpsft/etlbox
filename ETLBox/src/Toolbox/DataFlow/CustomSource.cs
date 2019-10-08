using System;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Define your own source block.
    /// </summary>
    /// <typeparam name="TOutput">Type of data output.</typeparam>
    public class CustomSource<TOutput> : DataFlowSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName => $"Dataflow: Custom source";
        public void Execute() => ExecuteAsync();

        /* Public properties */
        public Func<TOutput> ReadFunc { get; set; }
        public Func<bool> ReadCompletedFunc { get; set; }

        /* Private stuff */

        public CustomSource()
        {
        }

        public CustomSource(Func<TOutput> readFunc, Func<bool> readCompletedFunc) : this()
        {
            ReadFunc = readFunc;
            ReadCompletedFunc = readCompletedFunc;
        }

        public CustomSource(string name, Func<TOutput> readFunc, Func<bool> readCompletedFunc) : this(readFunc, readCompletedFunc)
        {
            this.TaskName = name;
        }

        public void ExecuteAsync()
        {
            NLogStart();
            while (!ReadCompletedFunc.Invoke())
            {
                Buffer.Post(ReadFunc.Invoke());
                LogProgress(1);
            }
            Buffer.Complete();
            NLogFinish();
        }
    }

    /// <summary>
    /// Define your own source block. The non generic implementation return a string array as output.
    /// </summary>
    public class CustomSource : CustomSource<string[]>
    {
        public CustomSource() : base()
        { }

        public CustomSource(Func<string[]> readFunc, Func<bool> readCompletedFunc) : base(readFunc, readCompletedFunc)
        { }

        public CustomSource(string name, Func<string[]> readFunc, Func<bool> readCompletedFunc) : base(name, readFunc, readCompletedFunc)
        { }
    }
}
