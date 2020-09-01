using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Transactions;

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
                    return OutputBuffer?.LastOrDefault();
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

        protected override void InternalInitBufferObjects()
        {
            if (Successors.Any(suc => suc.MaxBufferSize > 0))
            {
                AvoidBroadcastBlock = true;
                OwnBroadcastBlock = new ActionBlock<TInput>(Broadcast, new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = MaxBufferSize
                });
            }
            else
            {
                BroadcastBlock = new BroadcastBlock<TInput>(Clone, new DataflowBlockOptions()
                {
                    BoundedCapacity = MaxBufferSize
                });
            }
        }

        internal override void LinkBuffers(DataFlowComponent successor, LinkPredicates linkPredicates)
        {
            if (AvoidBroadcastBlock)
            {
                var buffer = new BufferBlock<TInput>(new DataflowBlockOptions()
                {
                    BoundedCapacity = MaxBufferSize
                });
                OutputBuffer.Add(buffer);
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
                    return Task.WhenAll(OutputBuffer.Select(b => b.Completion));
                else
                   return ((IDataflowBlock)BroadcastBlock).Completion;
            }
        }

        internal override void CompleteBufferOnPredecessorCompletion()
        {
            if (AvoidBroadcastBlock)
            {
                OwnBroadcastBlock.Complete();
                OwnBroadcastBlock.Completion.Wait();
                foreach (var buffer in OutputBuffer)
                    buffer.Complete();
            }
            else
            {
                BroadcastBlock.Complete();
            }
        }

        internal override void FaultBufferOnPredecessorCompletion(Exception e)
        {
            if (AvoidBroadcastBlock)
            {
                ((IDataflowBlock)OwnBroadcastBlock).Fault(e);
                OwnBroadcastBlock.Completion.Wait();
                foreach (var buffer in OutputBuffer)
                    ((IDataflowBlock)buffer).Fault(e);
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
        List<BufferBlock<TInput>> OutputBuffer = new List<BufferBlock<TInput>>();
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
                if (!buffer.SendAsync(clone).Result)
                    throw new ETLBoxException("Buffer already completed or faulted!", this.Exception);
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
