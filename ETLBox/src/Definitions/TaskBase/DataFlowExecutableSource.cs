using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// Base implementation for a source that can be executed.
    /// </summary>
    /// <typeparam name="TOutput"></typeparam>
    public abstract class DataFlowExecutableSource<TOutput> : DataFlowSource<TOutput>, IDataFlowExecutableSource<TOutput>
    {
        #region Buffer and completion

        /// <inheritdoc/>
        public override ISourceBlock<TOutput> SourceBlock => this.Buffer;
        protected BufferBlock<TOutput> Buffer { get; set; } = new BufferBlock<TOutput>();
        internal override Task BufferCompletion => Buffer.Completion;
        protected override void InitComponent()
        {
            Buffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize,
                CancellationToken = this.CancellationSource.Token
            });
            ComponentCompletion = new Task(
               () =>
               {
                   try
                   {
                       OnExecutionDoAsyncWork();
                       CompleteBuffer();
                       ErrorSource?.CompleteBuffer();
                       CleanUpOnSuccess();
                   }
                   catch (Exception e)
                   {
                       FaultBuffer(e);
                       ErrorSource?.FaultBuffer(e);
                       CleanUpOnFaulted(e);
                       throw e;
                   }
               }
               , TaskCreationOptions.LongRunning);
        }

        internal override void CompleteBuffer() => SourceBlock.Complete();
        internal override void FaultBuffer(Exception e) => SourceBlock.Fault(e);

        #endregion

        #region Execution and IDataFlowExecutableSource

        /// <inheritdoc/>
        public void Execute()
        {
            InitNetworkRecursively();
            OnExecutionDoSynchronousWork();
            ComponentCompletion.RunSynchronously();
        }

        /// <inheritdoc/>
        public Task ExecuteAsync()
        {
            InitNetworkRecursively();
            OnExecutionDoSynchronousWork();
            ComponentCompletion.Start();
            return Completion;
        }

        protected virtual void OnExecutionDoSynchronousWork() { } //abstract? Corner-case for ErrorSource

        protected virtual void OnExecutionDoAsyncWork() { } //abstract? Corner-Case for ErrorSource

        #endregion
    }
}
