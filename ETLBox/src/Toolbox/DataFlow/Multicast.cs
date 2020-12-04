using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ETLBox.DataFlow.Transformations
{
    /// <summary>
    /// A multicast broadcast data from the input into two or more outputs.
    /// Every linked component will receive a copy of the rows that the Multicast receives.
    /// There is no limit how many target the Multicast can be linked to.
    /// </summary>
    /// <typeparam name="TInput">Type of ingoing data.</typeparam>
    /// <example>
    /// <code>
    /// Multicast&lt;MyDataRow&gt; multicast = new Multicast&lt;MyDataRow&gt;();
    /// multicast.LinkTo(dest1);
    /// multicast.LinkTo(dest2);
    /// multicast.LinkTo(dest3);
    /// </code>
    /// </example>
    public class Multicast<TInput> : DataFlowTransformation<TInput, TInput>
    {
        #region Public properties

        /// <inheritdoc />
        public override string TaskName { get; set; } = "Multicast - duplicate data";
        /// <inheritdoc/>
        public override ISourceBlock<TInput> SourceBlock
        {
            get
            {
                if (AvoidBroadcastBlock)
                    return OutputBuffer?.LastOrDefault().Item1;
                else
                    return BroadcastBlock;
            }
        }
        /// <inheritdoc/>
        public override ITargetBlock<TInput> TargetBlock =>
            AvoidBroadcastBlock ? (ITargetBlock<TInput>)OwnBroadcastBlock : BroadcastBlock;

        #endregion

        #region Constructors

        public Multicast()
        {
            TypeInfo = new TypeInfo(typeof(TInput)).GatherTypeInfo();
            ObjectCopy = new ObjectCopy<TInput>(TypeInfo);
        }

        #endregion

        #region Implement abstract methods

        protected override void CheckParameter() { }

        protected override void InitComponent()
        {
            if (Successors.Any(suc => suc.MaxBufferSize > 0))
            {
                AvoidBroadcastBlock = true;
                OwnBroadcastBlock = new ActionBlock<TInput>(Broadcast, new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = MaxBufferSize,
                    CancellationToken = this.CancellationSource.Token
                });                
            }
            else
            {
                BroadcastBlock = new BroadcastBlock<TInput>(Clone, new DataflowBlockOptions()
                {
                    BoundedCapacity = MaxBufferSize,
                    CancellationToken = this.CancellationSource.Token
                });
            }
        }

        internal override void LinkBuffers(DataFlowComponent successor, LinkPredicates linkPredicates)
        {
            if (AvoidBroadcastBlock)
            {
                var buffer = new BufferBlock<TInput>(new DataflowBlockOptions()
                {
                    BoundedCapacity = MaxBufferSize,
                    CancellationToken = this.CancellationSource.Token
                });
                OutputBuffer.Add(Tuple.Create(buffer, linkPredicates));
                _bufferCompletion = Task.WhenAll(OutputBuffer.Select(b => b.Item1.Completion));
            }
            base.LinkBuffers(successor, linkPredicates);
        }

        protected override void CleanUpOnSuccess()
        {
            NLogFinishOnce();
        }

        protected override void CleanUpOnFaulted(Exception e) { }

        internal override Task BufferCompletion
        {
            get
            {
                if (AvoidBroadcastBlock)
                    return _bufferCompletion; //BufferCompletion is called twice, make sure to return always same task!
                    //return Task.WhenAll(OutputBuffer.Select(b => b.Item1.Completion));
                else
                    return ((IDataflowBlock)BroadcastBlock).Completion;
            }
        }
        Task _bufferCompletion;

        internal override void CompleteBuffer()
        {
            if (AvoidBroadcastBlock)
            {
                OwnBroadcastBlock.Complete();
                try 
                {
                    //Completion may be canceled!
                    if (!OwnBroadcastBlock.Completion.IsCanceled)
                        OwnBroadcastBlock.Completion.Wait(CancellationSource.Token); //Will throw exception as soon as the task is faulted!                
                }
                catch (Exception e) {
                //    FaultBuffer(e);
                //    throw e;
                }
                foreach (var buffer in OutputBuffer)
                    buffer.Item1.Complete();
            }
            else
            {
                BroadcastBlock.Complete();
            }
        }

        internal override void FaultBuffer(Exception e)
        {
            if (AvoidBroadcastBlock)
            {
                ((IDataflowBlock)OwnBroadcastBlock).Fault(e); //Will fault task, but not immediately
                try //A faulted task can't be waited on, so Exception is ignored
                {
                    OwnBroadcastBlock.Completion.Wait(CancellationSource.Token); //Will throw exception as soon as the task is faulted!
                }
                catch { }
                foreach (var buffer in OutputBuffer) //Keep order
                    ((IDataflowBlock)buffer.Item1).Fault(e);
            }
            else
            {
                ((IDataflowBlock)BroadcastBlock).Fault(e);
            }
        }

        #endregion

        #region Implementation

        bool AvoidBroadcastBlock;
        BroadcastBlock<TInput> BroadcastBlock;
        ActionBlock<TInput> OwnBroadcastBlock;
        List<Tuple<BufferBlock<TInput>, LinkPredicates>> OutputBuffer = new List<Tuple<BufferBlock<TInput>, LinkPredicates>>();
        TypeInfo TypeInfo;
        ObjectCopy<TInput> ObjectCopy;

        private TInput Clone(TInput row)
        {
            NLogStartOnce();
            TInput clone = ObjectCopy.Clone(row);
            LogProgress();
            return clone;
        }

        private void Broadcast(TInput row)
        {
            TInput clone = Clone(row);
            foreach (var buffer in OutputBuffer)
            {
                var lp = buffer.Item2;
                var pk = lp.PredicateKeep as Predicate<TInput>;
                if (!lp.HasPredicate || (lp.HasPredicate && pk.Invoke(clone)))
                {
                    if (!buffer.Item1.SendAsync(clone).Result)
                    {
                        if (CancellationSource.IsCancellationRequested)
                            throw new System.Threading.Tasks.TaskCanceledException();
                        else
                            throw new ETLBoxFaultedBufferException();
                    }
                }
            }
        }

        #endregion
    }

    /// <inheritdoc/>
    public class Multicast : Multicast<ExpandoObject>
    {
        public Multicast() : base() { }
    }
}
