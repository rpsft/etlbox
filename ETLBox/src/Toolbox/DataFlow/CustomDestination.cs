using ETLBox.ControlFlow;
using System;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// Define your own destination block. This block accepts all data from the flow and sends it to your custom written Action.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    public class CustomDestination<TInput> : DataFlowDestination<TInput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = $"Write data into custom target";
        /// <summary>
        /// Each row that the CustomDestination receives is send into this Action.
        /// </summary>
        public Action<TInput> WriteAction { get; set; }

        #endregion

        #region Constructros

        public CustomDestination()
        {

        }

        /// <param name="writeAction">Sets the <see cref="WriteAction"/></param>
        public CustomDestination(Action<TInput> writeAction) : this()
        {
            WriteAction = writeAction;
        }

        #endregion

        #region Implement abstract methods

        protected override void InternalInitBufferObjects()
        {
            TargetAction = new ActionBlock<TInput>(AddLoggingAndErrorHandling(WriteAction), new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }
        protected override void CleanUpOnFaulted(Exception e) { }

        #endregion

        #region Implementation

        private Action<TInput> AddLoggingAndErrorHandling(Action<TInput> writeAction)
        {
            return new Action<TInput>(
                input =>
                {
                    NLogStartOnce();
                    try
                    {
                        if (input != null)
                            writeAction.Invoke(input);
                    }
                    catch (Exception e)
                    {
                        ThrowOrRedirectError(e, ErrorSource.ConvertErrorData<TInput>(input));
                    }
                    LogProgress();
                });
        }

        #endregion
    }

    /// <inheritdoc/>
    public class CustomDestination : CustomDestination<ExpandoObject>
    {
        /// <inheritdoc/>
        public CustomDestination() : base()
        { }

        /// <inheritdoc/>
        public CustomDestination(Action<ExpandoObject> writeAction) : base(writeAction)
        { }
    }
}
