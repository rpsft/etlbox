using ETLBox.Exceptions;
using System;
using System.Threading;
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
        
        protected Task SourceTask;
        protected virtual bool CompleteManually { get; set; }

        protected override void InitComponent()
        {            
            Buffer = new BufferBlock<TOutput>(new DataflowBlockOptions()
            {
                BoundedCapacity = MaxBufferSize,
                CancellationToken = CancellationSource.Token,
            });

            SourceTask = new Task(
             () =>
             {
                 OnExecutionDoAsyncWork();
             }
             , CSSource.Token, TaskCreationOptions.LongRunning);

            if (this.GetType() == typeof(ErrorSource) && CompleteManually == false)
                throw new InvalidOperationException("Errors Source is always completed manually!");
            if (CompleteManually) 
                SourceOrPredecessorCompletion = SourceTask;
            else
                SourceOrPredecessorCompletion = SourceTask.ContinueWith(t =>
             this.CompleteOrFaultBufferOnPredecessorCompletion(t), CSCont.Token);

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
            if (!SourceTask.IsCompleted) //Needed if other parts of the network already canceled this source
                SourceTask.RunSynchronously();
        }

        /// <inheritdoc/>
        public Task ExecuteAsync()
        {
            InitNetworkRecursively();
            OnExecutionDoSynchronousWork();
            if (!SourceTask.IsCompleted) //Needed if other parts of the network already canceled this source
                SourceTask.Start();
            return Completion;
        }

        protected abstract void OnExecutionDoSynchronousWork();

        protected abstract void OnExecutionDoAsyncWork();

        #endregion
    }
}
