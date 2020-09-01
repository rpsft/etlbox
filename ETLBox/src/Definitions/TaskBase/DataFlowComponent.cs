using ETLBox.ControlFlow;
using ETLBox.Exceptions;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using TSQL.Clauses;

namespace ETLBox.DataFlow
{
    /// <summary>
    /// A base class for data flow components
    /// </summary>
    public abstract class DataFlowComponent : LoggableTask, IDataFlowComponent, IDataFlowLogging
    {
        #region Component properties

        /// <inheritdoc/>
        public int MaxBufferSize
        {
            get
            {
                return _maxBufferSize > 0 ? _maxBufferSize : DataFlow.MaxBufferSize;
            }
            set
            {
                _maxBufferSize = value;
            }
        }

        protected int _maxBufferSize = -1;

        #endregion

        #region Linking

        /// <summary>
        /// All predecessor that are linked to this component.
        /// </summary>
        public List<DataFlowComponent> Predecessors { get; protected set; } = new List<DataFlowComponent>();

        /// <summary>
        /// All successor that this component is linked to.
        /// </summary>
        public List<DataFlowComponent> Successors { get; protected set; } = new List<DataFlowComponent>();

        /// <inheritdoc/>
        public Task Completion { get; internal set; }
        internal virtual Task BufferCompletion { get; }
        protected Task PredecessorCompletion { get; set; }
        protected DataFlowComponent Parent { get; set; }

        protected bool WereBufferInitialized;
        protected bool ReadyForProcessing;
        protected Dictionary<DataFlowComponent, bool> WasLinked = new Dictionary<DataFlowComponent, bool>();
        internal Dictionary<DataFlowComponent, LinkPredicates> LinkPredicates = new Dictionary<DataFlowComponent, LinkPredicates>();

        protected IDataFlowSource<T> InternalLinkTo<T>(IDataFlowDestination target, object predicate = null, object voidPredicate = null)
        {
            DataFlowComponent tgt = target as DataFlowComponent;
            LinkPredicates.Add(tgt, new LinkPredicates(predicate, voidPredicate));
            this.Successors.Add(tgt);
            tgt.Predecessors.Add(this);
            var res = target as IDataFlowSource<T>;
            return res;
        }

        protected void LinkBuffersRecursively()
        {
            foreach (DataFlowComponent predecessor in Predecessors)
            {
                if (!predecessor.WasLinked.ContainsKey(this))
                {
                    LinkPredicates predicate = null;
                    LinkPredicates.TryGetValue(this, out predicate);
                    predecessor.LinkBuffers(this, predicate);
                    predecessor.WasLinked.Add(this, true);
                    predecessor.LinkBuffersRecursively();
                }
            }
            foreach (DataFlowComponent successor in Successors)
            {
                if (!WasLinked.ContainsKey(successor))
                {
                    LinkPredicates predicate = null;
                    LinkPredicates.TryGetValue(successor, out predicate);
                    LinkBuffers(successor, predicate);
                    WasLinked.Add(successor, true);
                    successor.LinkBuffersRecursively();
                }
            }
        }
        internal virtual void LinkBuffers(DataFlowComponent successor, LinkPredicates predicate)
        {
            //No linking by default
        }

        #endregion

        #region Network initialization

        internal void InitNetworkRecursively()
        {
            InitBufferRecursively();
            LinkBuffersRecursively();
            SetCompletionTaskRecursively();
            RunErrorSourceInitializationRecursively();
        }


        protected void InitBufferRecursively()
        {
            foreach (DataFlowComponent predecessor in Predecessors)
                if (!predecessor.WereBufferInitialized)
                    predecessor.InitBufferRecursively();

            if (!WereBufferInitialized)
            {
                InternalInitBufferObjects();
                WereBufferInitialized = true;
            }

            foreach (DataFlowComponent successor in Successors)
                if (!successor.WereBufferInitialized)
                    successor.InitBufferRecursively();
        }

        /// <summary>
        /// Inits the underlying TPL.Dataflow buffer objects. After this, the component is ready for linking
        /// its source or target blocks.
        /// </summary>
        public void InitBufferObjects() {
            InternalInitBufferObjects();
            WereBufferInitialized = true;
        }

        protected abstract void InternalInitBufferObjects();

        protected void RunErrorSourceInitializationRecursively()
        {
            foreach (DataFlowComponent predecessor in Predecessors)
                if (!predecessor.ReadyForProcessing)
                    predecessor.RunErrorSourceInitializationRecursively();

            if (!ReadyForProcessing)
            {
                LetErrorSourceWaitForInput();
                ReadyForProcessing = true;
            }

            foreach (DataFlowComponent successor in Successors)
                if (!successor.ReadyForProcessing)
                    successor.RunErrorSourceInitializationRecursively();
        }

        #endregion

        #region Completion tasks handling

        /// <inheritdoc/>
        public Action OnCompletion { get; set; }

        protected void SetCompletionTaskRecursively()
        {
            foreach (DataFlowComponent predecessor in Predecessors)
                if (predecessor.Completion == null)
                    predecessor.SetCompletionTaskRecursively();

            if (Completion == null)
            {
                List<Task> PredecessorCompletionTasks = CollectCompletionFromPredecessors();
                if (PredecessorCompletionTasks.Count > 0)
                {
                    PredecessorCompletion = Task.WhenAll(PredecessorCompletionTasks).ContinueWith(CompleteOrFaultOnPredecessorCompletion);
                    Completion = Task.WhenAll(PredecessorCompletion, BufferCompletion).ContinueWith(CompleteOrFaultCompletion);
                }
            }

            foreach (DataFlowComponent successor in Successors)
                if (successor.Completion == null)
                    successor.SetCompletionTaskRecursively();
        }

        private List<Task> CollectCompletionFromPredecessors()
        {
            List<Task> CompletionTasks = new List<Task>();
            foreach (DataFlowComponent pre in Predecessors)
            {
                CompletionTasks.Add(pre.Completion);
                CompletionTasks.Add(pre.BufferCompletion);
            }
            return CompletionTasks;
        }

        /// <summary>
        /// Predecessor completion task (Buffer of predecessors and Completion of predecessors) ran to completion or are faulted.
        /// Now complete or fault the current buffer.
        /// </summary>
        /// <param name="t">t is the continuation of Task.WhenAll of the predecessors buffer and predecessor completion tasks</param>
        protected void CompleteOrFaultOnPredecessorCompletion(Task t)
        {
            if (t.IsFaulted)
            {
                FaultBufferOnPredecessorCompletion(t.Exception.InnerException);
                throw t.Exception.InnerException;
            }
            else
            {
                CompleteBufferOnPredecessorCompletion();
            }
        }

        internal abstract void CompleteBufferOnPredecessorCompletion();
        internal abstract void FaultBufferOnPredecessorCompletion(Exception e);

        protected void CompleteOrFaultCompletion(Task t)
        {
            LetErrorSourceFinishUp();
            if (t.IsFaulted)
            {
                CleanUpOnFaulted(t.Exception.InnerException);
                throw t.Exception.InnerException; //Will fault Completion task
            }
            else
            {
                CleanUpOnSuccess();
                OnCompletion?.Invoke();
            }
        }

        protected virtual void CleanUpOnSuccess() { }

        protected virtual void CleanUpOnFaulted(Exception e) { }

        protected void FaultPredecessorsRecursively(Exception e)
        {
            Exception = e;
            FaultBufferOnPredecessorCompletion(e);
            foreach (DataFlowComponent pre in Predecessors)
                pre.FaultPredecessorsRecursively(e);
        }

        #endregion

        #region Error Handling

        /// <inheritdoc/>
        public Exception Exception { get; set; }

        /// <summary>
        /// The ErrorSource is the source block used for sending errors into the linked error flow.
        /// </summary>
        public ErrorSource ErrorSource { get; set; }

        protected IDataFlowSource<ETLBoxError> InternalLinkErrorTo(IDataFlowDestination<ETLBoxError> target)
        {
            if (ErrorSource == null)
                ErrorSource = new ErrorSource();
            ErrorSource.LinkTo(target);
            return target as IDataFlowSource<ETLBoxError>;
        }

        protected void ThrowOrRedirectError(Exception e, string message)
        {
            if (ErrorSource == null)
            {
                FaultPredecessorsRecursively(e);
                throw e;
            }
            ErrorSource.Send(e, message);
        }

        private void LetErrorSourceWaitForInput() =>
            ErrorSource?.ExecuteAsync().Wait();

        private void LetErrorSourceFinishUp() =>
             ErrorSource?.CompleteBufferOnPredecessorCompletion();

        #endregion

        #region Logging

        protected int? _loggingThresholdRows;

        /// <inheritdoc/>
        public virtual int? LoggingThresholdRows
        {
            get
            {
                if ((DataFlow.LoggingThresholdRows ?? 0) > 0)
                    return DataFlow.LoggingThresholdRows;
                else
                    return _loggingThresholdRows;
            }
            set
            {
                _loggingThresholdRows = value;
            }
        }

        /// <inheritdoc/>
        public int ProgressCount { get; set; }

        protected bool HasLoggingThresholdRows => LoggingThresholdRows != null && LoggingThresholdRows > 0;
        protected int ThresholdCount { get; set; } = 1;
        protected bool WasLoggingStarted;
        protected bool WasLoggingFinished;

        protected void NLogStartOnce()
        {
            if (!WasLoggingStarted)
                NLogStart();
            WasLoggingStarted = true;
        }
        protected void NLogFinishOnce()
        {
            if (WasLoggingStarted && !WasLoggingFinished)
                NLogFinish();
            WasLoggingFinished = true;
        }
        private void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, Logging.Logging.STAGE, Logging.Logging.CurrentLoadProcess?.Id);
        }

        private void NLogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                NLogger.Info(TaskName + $" processed {ProgressCount} records in total.", TaskType, "LOG", TaskHash, Logging.Logging.STAGE, Logging.Logging.CurrentLoadProcess?.Id);
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "END", TaskHash, Logging.Logging.STAGE, Logging.Logging.CurrentLoadProcess?.Id);
        }

        protected void LogProgressBatch(int rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (!DisableLogging && HasLoggingThresholdRows && ProgressCount >= (LoggingThresholdRows * ThresholdCount))
            {
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, Logging.Logging.STAGE, Logging.Logging.CurrentLoadProcess?.Id);
                ThresholdCount++;
            }
        }

        protected void LogProgress()
        {
            ProgressCount += 1;
            if (!DisableLogging && HasLoggingThresholdRows && (ProgressCount % LoggingThresholdRows == 0))
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, Logging.Logging.STAGE, Logging.Logging.CurrentLoadProcess?.Id);
        }
        #endregion
    }
}
