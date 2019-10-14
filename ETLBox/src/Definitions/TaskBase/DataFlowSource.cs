using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.Helper;
using ALE.ETLBox.ControlFlow;
using System;
using ALE.ETLBox.Logging;
using System.Threading.Tasks.Dataflow;
using ALE.ETLBox.DataFlow;

namespace ALE.ETLBox.DataFlow {
    public abstract class DataFlowSource<TOutput> : DataFlowTask, ITask {
        public ISourceBlock<TOutput> SourceBlock => this.Buffer;
        internal BufferBlock<TOutput> Buffer { get; set; } = new BufferBlock<TOutput>();
        internal TypeInfo TypeInfo { get; set; }

        public DataFlowSource()
        {
            TypeInfo = new TypeInfo(typeof(TOutput));
        }

        public void LinkTo(IDataFlowLinkTarget<TOutput> target)
        {
            Buffer.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true });
            if (!DisableLogging)
                NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        public void LinkTo(IDataFlowLinkTarget<TOutput> target, Predicate<TOutput> predicate)
        {
            Buffer.LinkTo(target.TargetBlock, new DataflowLinkOptions() { PropagateCompletion = true }, predicate);
            if (!DisableLogging)
                NLogger.Debug(TaskName + " was linked to Target!", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        internal void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        internal void NLogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                NLogger.Info(TaskName + $" processed {ProgressCount} records in total.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        internal void LogProgress(int rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (!DisableLogging && HasLoggingThresholdRows && (ProgressCount % LoggingThresholdRows == 0))
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }
    }
}
