using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Connectors
{

    /// <summary>
    /// Define a source based on a generic .NET collection. This could be a List&lt;T&gt; or any other IEnumerable&lt;T&gt;.
    /// By default, an empty List&lt;T&gt; is created which can be filled with data.
    /// </summary>
    /// <typeparam name="TOutput">Type of outgoing data.</typeparam>
    public class MemorySource<TOutput> : DataFlowExecutableSource<TOutput>
    {
        #region Public properties

        /// <inheritdoc/>
        public override string TaskName => $"Read data from memory";

        /// The .NET collection that is used to read the data from.
        public IEnumerable<TOutput> Data { get; set; }

        /// If the source collection implements IList&lt;T&gt; then this property will convert the collection into this interface type.
        public IList<TOutput> DataAsList
        {
            get
            {
                return Data as IList<TOutput>;
            }
            set
            {
                Data = value;
            }
        }

        #endregion

        #region Constructors

        public MemorySource()
        {
            Data = new List<TOutput>();
        }

        /// <param name="data">Set the source collection and stores it in <see cref="Data"/></param>
        public MemorySource(IEnumerable<TOutput> data)
        {
            Data = data;
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
            foreach (TOutput record in Data)
            {
                if (!Buffer.SendAsync(record).Result)
                    throw new ETLBoxException("Buffer already completed or faulted!", this.Exception);
                LogProgress();
            }
        }

        #endregion
    }

    /// <inheritdoc/>
    public class MemorySource : MemorySource<ExpandoObject>
    {
        public MemorySource() : base() { }
        public MemorySource(IList<ExpandoObject> data) : base(data) { }
    }
}
