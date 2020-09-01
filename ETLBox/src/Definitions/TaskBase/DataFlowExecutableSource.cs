using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using NLog.Targets;
using System;
using System.Linq.Expressions;
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
        protected override void InternalInitBufferObjects()
        {
            Buffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize
            });
            Completion = new Task(
               () =>
               {
                   try
                   {
                       OnExecutionDoAsyncWork();
                       CompleteBufferOnPredecessorCompletion();
                       ErrorSource?.CompleteBufferOnPredecessorCompletion();
                       CleanUpOnSuccess();
                   }
                   catch (Exception e)
                   {
                       FaultBufferOnPredecessorCompletion(e);
                       ErrorSource?.FaultBufferOnPredecessorCompletion(e);
                       CleanUpOnFaulted(e);
                       throw e;
                   }
               }
               , TaskCreationOptions.LongRunning);
        }

        internal override void CompleteBufferOnPredecessorCompletion() => SourceBlock.Complete();
        internal override void FaultBufferOnPredecessorCompletion(Exception e) => SourceBlock.Fault(e);

        #endregion

        #region Execution and IDataFlowExecutableSource

        /// <inheritdoc/>
        public void Execute()
        {
            InitNetworkRecursively();
            OnExecutionDoSynchronousWork();
            Completion.RunSynchronously();
        }

        /// <inheritdoc/>
        public Task ExecuteAsync()
        {
            InitNetworkRecursively();
            OnExecutionDoSynchronousWork();
            Completion.Start();
            return Completion;
        }

        protected virtual void OnExecutionDoSynchronousWork() { } //abstract? Corner-case for ErrorSource

        protected virtual void OnExecutionDoAsyncWork() { } //abstract? Corner-Case for ErrorSource

        #endregion
    }
}
