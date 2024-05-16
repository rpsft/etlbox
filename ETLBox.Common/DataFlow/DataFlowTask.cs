using System.Diagnostics.CodeAnalysis;
using ALE.ETLBox.Common.ControlFlow;
using JetBrains.Annotations;

namespace ALE.ETLBox.Common.DataFlow
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
                return Common.DataFlow.DataFlow.HasLoggingThresholdRows
                    ? Common.DataFlow.DataFlow.LoggingThresholdRows
                    : _loggingThresholdRows;
            }
            set { _loggingThresholdRows = value; }
        }

        public int ProgressCount { get; set; }

        protected bool HasLoggingThresholdRows => LoggingThresholdRows is > 0;
        protected int ThresholdCount { get; set; } = 1;

        protected void LogStart()
        {
            if (!DisableLogging)
                Logger.Info(
                    TaskName,
                    TaskType,
                    "START",
                    TaskHash,
                    ControlFlow.ControlFlow.Stage,
                    ControlFlow.ControlFlow.CurrentLoadProcess?.Id
                );
        }

        protected void LogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                Logger.Info(
                    TaskName + $" processed {ProgressCount} records in total.",
                    TaskType,
                    "LOG",
                    TaskHash,
                    ControlFlow.ControlFlow.Stage,
                    ControlFlow.ControlFlow.CurrentLoadProcess?.Id
                );
            if (!DisableLogging)
                Logger.Info(
                    TaskName,
                    TaskType,
                    "END",
                    TaskHash,
                    ControlFlow.ControlFlow.Stage,
                    ControlFlow.ControlFlow.CurrentLoadProcess?.Id
                );
        }

        protected void LogProgressBatch(int rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (
                DisableLogging
                || !HasLoggingThresholdRows
                || ProgressCount < LoggingThresholdRows * ThresholdCount
            )
            {
                return;
            }

            Logger.Info(
                TaskName + $" processed {ProgressCount} records.",
                TaskType,
                "LOG",
                TaskHash,
                ControlFlow.ControlFlow.Stage,
                ControlFlow.ControlFlow.CurrentLoadProcess?.Id
            );
            ThresholdCount++;
        }

        protected void LogProgress()
        {
            ProgressCount += 1;
            if (
                DisableLogging
                || !HasLoggingThresholdRows
                || ProgressCount % LoggingThresholdRows != 0
            )
            {
                return;
            }

            Logger.Info(
                TaskName + $" processed {ProgressCount} records.",
                TaskType,
                "LOG",
                TaskHash,
                ControlFlow.ControlFlow.Stage,
                ControlFlow.ControlFlow.CurrentLoadProcess?.Id
            );
        }
    }
}
