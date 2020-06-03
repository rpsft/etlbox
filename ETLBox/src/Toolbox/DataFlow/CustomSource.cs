using System;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// Define your own source block.
    /// </summary>
    /// <typeparam name="TOutput">Type of data output.</typeparam>
    public class CustomSource<TOutput> : DataFlowSource<TOutput>, ITask, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public override string TaskName => $"Read data from custom source";


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

        public override void Execute()
        {
            NLogStart();
            while (!ReadCompletedFunc.Invoke())
            {
                try
                {
                    Buffer.SendAsync(ReadFunc.Invoke()).Wait();
                }
                catch (Exception e)
                {
                    if (!ErrorHandler.HasErrorBuffer) throw e;
                    ErrorHandler.Send(e, e.Message);
                }
                LogProgress();
            }
            Buffer.Complete();
            NLogFinish();
        }

    }

    /// <summary>
    /// Define your own source block. The non generic implementation returns a dynamic object as output.
    /// </summary>
    public class CustomSource : CustomSource<ExpandoObject>
    {
        public CustomSource() : base()
        { }

        public CustomSource(Func<ExpandoObject> readFunc, Func<bool> readCompletedFunc) : base(readFunc, readCompletedFunc)
        { }

        public CustomSource(string name, Func<ExpandoObject> readFunc, Func<bool> readCompletedFunc) : base(name, readFunc, readCompletedFunc)
        { }
    }
}
