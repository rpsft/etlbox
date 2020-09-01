using ETLBox.ControlFlow;
using NLog.Targets.Wrappers;
using System;
using System.Dynamic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// This destination serves as a recycle bin for data that is not supposed to go into any other destination.
    /// Every records in the dataflow needs to enter any kind of destination in order to have a dataflow completed.
    /// Use this target for data that you don't want to use in a destination, but you still want your dataflow to complete property.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    public class VoidDestination<TInput> : DataFlowDestination<TInput>
    {
        #region Public properties

        /// <inheritdoc />
        public override string TaskName => $"Void destination - Ignore data";

        #endregion

        #region Constructors

        public VoidDestination()
        {

        }

        #endregion

        #region Implement abstract interfaces

        protected override void InternalInitBufferObjects()
        {
            TargetAction = new ActionBlock<TInput>(r => { });
        }

        protected override void CleanUpOnSuccess()
        {
        }

        protected override void CleanUpOnFaulted(Exception e)
        {
        }
        #endregion

    }

    /// <inheritdoc/>
    public class VoidDestination : VoidDestination<ExpandoObject>
    {
        public VoidDestination() : base()
        { }
    }
}
