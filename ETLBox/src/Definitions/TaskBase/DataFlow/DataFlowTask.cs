using System.Diagnostics.CodeAnalysis;

namespace ALE.ETLBox
{
    [PublicAPI]
    [SuppressMessage("ReSharper", "TemplateIsNotCompileTimeConstantProblem")]
    public abstract class DataFlowTask : GenericTask
    {
        private int? _loggingThresholdRows;
        public virtual int? LoggingThresholdRows
        {
            get
            {
                if (DataFlow.DataFlow.HasLoggingThresholdRows)
                    return DataFlow.DataFlow.LoggingThresholdRows;
                return _loggingThresholdRows;
            }
            set { _loggingThresholdRows = value; }
        }

        public int ProgressCount { get; set; }

        protected bool HasLoggingThresholdRows => LoggingThresholdRows is > 0;
        protected int ThresholdCount { get; set; } = 1;

        protected void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(
                    TaskName,
                    TaskType,
                    "START",
                    TaskHash,
                    ControlFlow.ControlFlow.STAGE,
                    ControlFlow.ControlFlow.CurrentLoadProcess?.Id
                );
        }

        protected void NLogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                NLogger.Info(
                    TaskName + $" processed {ProgressCount} records in total.",
                    TaskType,
                    "LOG",
                    TaskHash,
                    ControlFlow.ControlFlow.STAGE,
                    ControlFlow.ControlFlow.CurrentLoadProcess?.Id
                );
            if (!DisableLogging)
                NLogger.Info(
                    TaskName,
                    TaskType,
                    "END",
                    TaskHash,
                    ControlFlow.ControlFlow.STAGE,
                    ControlFlow.ControlFlow.CurrentLoadProcess?.Id
                );
        }

        protected void LogProgressBatch(int rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (
                !DisableLogging
                && HasLoggingThresholdRows
                && ProgressCount >= LoggingThresholdRows * ThresholdCount
            )
            {
                NLogger.Info(
                    TaskName + $" processed {ProgressCount} records.",
                    TaskType,
                    "LOG",
                    TaskHash,
                    ControlFlow.ControlFlow.STAGE,
                    ControlFlow.ControlFlow.CurrentLoadProcess?.Id
                );
                ThresholdCount++;
            }
        }

        protected void LogProgress()
        {
            ProgressCount += 1;
            if (
                !DisableLogging
                && HasLoggingThresholdRows
                && ProgressCount % LoggingThresholdRows == 0
            )
                NLogger.Info(
                    TaskName + $" processed {ProgressCount} records.",
                    TaskType,
                    "LOG",
                    TaskHash,
                    ControlFlow.ControlFlow.STAGE,
                    ControlFlow.ControlFlow.CurrentLoadProcess?.Id
                );
        }
    }
}
