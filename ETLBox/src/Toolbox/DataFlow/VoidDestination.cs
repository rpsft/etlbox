using System;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// This destination if used as a trash.
    /// Redirect all data in this destination which you do not want for further processing.
    /// Every records needs to be transferred to a destination to have a dataflow completed.
    /// </summary>
    /// <typeparam name="TInput">Type of datasoure input.</typeparam>
    public class VoidDestination<TInput> : DataFlowTask, ITask, IDataFlowDestination<TInput>
    {

        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_VOIDDEST";
        public override string TaskName => $"Dataflow: Ignore data";
        public override void Execute() { throw new Exception("Dataflow destinations can't be started directly"); }

        /* Public properties */
        public ITargetBlock<TInput> TargetBlock => _voidDestination?.TargetBlock;

        /* Private stuff */
        CustomDestination<TInput> _voidDestination { get; set; }
        NLog.Logger NLogger { get; set; }

        public VoidDestination()
        {
            NLogger = NLog.LogManager.GetLogger("ETL");
            _voidDestination = new CustomDestination<TInput>(this, row => {; });
        }

        public void Wait() => _voidDestination.Wait();
    }

    /// <summary>
    /// This destination if used as a trash.
    /// Redirect all data in this destination which you do not want for further processing.
    /// Every records needs to be transferred to a destination to have a dataflow completed.
    /// The non generic implementation works with a string array as input.
    /// </summary>
    public class VoidDestination : VoidDestination<string[]>
    {
        public VoidDestination() : base()
        { }
    }
}
