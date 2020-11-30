using System;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// Define your own destination block. This block accepts all data from the flow and sends each incoming row to your custom Action, along with a count of processed rows. 
    /// </summary>
    /// <example>
    /// <code>
    /// List<MyRow> rows = new List<MyRow>();
    //  var dest = new CustomDestination<MyRow>();
    //  dest.WriteAction = (row, progressCount) => rows.Add(row);
    /// </code>
    /// </example>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    public class CustomDestination<TInput> : DataFlowDestination<TInput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName { get; set; } = $"Write data into custom target";
        /// <summary>
        /// Each row that the CustomDestination receives is send into this Action as first input value. The second input value is the current progress count.
        /// </summary>
        public Action<TInput,int> WriteAction { get; set; }

        #endregion

        #region Constructors

        public CustomDestination()
        {

        }

        /// <param name="writeAction">Sets the <see cref="WriteAction"/></param>
        public CustomDestination(Action<TInput,int> writeAction) : this()
        {
            WriteAction = writeAction;
        }

        #endregion

        #region Implement abstract methods

        protected override void CheckParameter() { }

        protected override void InitComponent()
        {
            TargetAction = new ActionBlock<TInput>(AddLoggingAndErrorHandling(WriteAction), new ExecutionDataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize,
                CancellationToken = this.CancellationSource.Token
            });
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }
        protected override void CleanUpOnFaulted(Exception e) { }

        #endregion

        #region Implementation

        private Action<TInput> AddLoggingAndErrorHandling(Action<TInput,int> writeAction)
        {
            return new Action<TInput>(
                input =>
                {
                    NLogStartOnce();
                    try
                    {
                        if (input != null)
                            writeAction.Invoke(input, ProgressCount);
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
        public CustomDestination(Action<ExpandoObject,int> writeAction) : base(writeAction)
        { }
    }
}
