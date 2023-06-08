using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Define your own source block.
    /// </summary>
    /// <typeparam name="TOutput">Type of data output.</typeparam>
    [PublicAPI]
    public class CustomSource<TOutput> : DataFlowSource<TOutput>, IDataFlowSource<TOutput>
    {
        /* ITask Interface */
        public sealed override string TaskName => "Read data from custom source";

        /* Public properties */
        public Func<TOutput> ReadFunc { get; set; }
        public Func<bool> ReadCompletedFunc { get; set; }

        /* Private stuff */

        public CustomSource() { }

        public CustomSource(Func<TOutput> readFunc, Func<bool> readCompletedFunc)
            : this()
        {
            ReadFunc = readFunc;
            ReadCompletedFunc = readCompletedFunc;
        }

        public CustomSource(string name, Func<TOutput> readFunc, Func<bool> readCompletedFunc)
            : this(readFunc, readCompletedFunc)
        {
            TaskName = name;
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
                    if (!ErrorHandler.HasErrorBuffer)
                        throw;
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
    [PublicAPI]
    public class CustomSource : CustomSource<ExpandoObject>
    {
        public CustomSource() { }

        public CustomSource(Func<ExpandoObject> readFunc, Func<bool> readCompletedFunc)
            : base(readFunc, readCompletedFunc) { }

        public CustomSource(string name, Func<ExpandoObject> readFunc, Func<bool> readCompletedFunc)
            : base(name, readFunc, readCompletedFunc) { }
    }
}
