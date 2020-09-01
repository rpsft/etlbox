using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using System;
using System.Dynamic;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{
    /// <summary>
    /// Define your own source block. This block generates data from a your own custom written functions.
    /// </summary>
    /// <typeparam name="TOutput">Type of outgoing data.</typeparam>
    public class CustomSource<TOutput> : DataFlowExecutableSource<TOutput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName => $"Read data from custom source";

        /// <summary>
        /// The Func that returns a data row as output.
        /// </summary>
        public Func<TOutput> ReadFunc { get; set; }

        /// <summary>
        /// This Func returns true when all rows for the flow are successfully returned from the <see cref="ReadFunc"/>.
        /// </summary>
        public Func<bool> ReadCompletedFunc { get; set; }

        #endregion

        #region Constructors

        public CustomSource()
        {
        }

        /// <param name="readFunc">Sets the <see cref="ReadFunc"/></param>
        /// <param name="readCompletedFunc">Sets the <see cref="ReadCompletedFunc"/></param>
        public CustomSource(Func<TOutput> readFunc, Func<bool> readCompletedFunc) : this()
        {
            ReadFunc = readFunc;
            ReadCompletedFunc = readCompletedFunc;
        }

        #endregion

        #region Implement abstract methods

        protected override void OnExecutionDoSynchronousWork() { }

        protected override void OnExecutionDoAsyncWork()
        {
            NLogStartOnce();
            ReadAllRecords();
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        #endregion

        #region Implementation

        private void ReadAllRecords()
        {
            while (!ReadCompletedFunc.Invoke())
            {
                TOutput result = default;
                try
                {
                    result = ReadFunc.Invoke();
                    if (!Buffer.SendAsync(result).Result)
                        throw new ETLBoxException("Buffer already completed or faulted!", this.Exception);
                }
                catch (ETLBoxException) { throw; }
                catch (Exception e)
                {
                    ThrowOrRedirectError(e, e.Message);
                }

                LogProgress();
            }
        }

        #endregion

    }

    /// <inheritdoc/>
    public class CustomSource : CustomSource<ExpandoObject>
    {
        public CustomSource() : base()
        { }

        public CustomSource(Func<ExpandoObject> readFunc, Func<bool> readCompletedFunc) : base(readFunc, readCompletedFunc)
        { }
    }
}
