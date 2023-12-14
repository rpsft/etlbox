using System.Diagnostics.CodeAnalysis;
using ALE.ETLBox.src.Toolbox.ControlFlow;

namespace ALE.ETLBox.src.Definitions.TaskBase
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
                return HasLoggingThresholdRows
                    ? LoggingThresholdRows
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
                Logger.Info<DataFlowTask>(
                    TaskName,
                    TaskType,
                    "START",
                    TaskHash,
                    Toolbox.ControlFlow.ControlFlow.Stage,
                    Toolbox.ControlFlow.ControlFlow.CurrentLoadProcess?.Id
                );
        }

        protected void LogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                Logger.Info<DataFlowTask>(
                    TaskName + $" processed {ProgressCount} records in total.",
                    TaskType,
                    "LOG",
                    TaskHash,
                    Toolbox.ControlFlow.ControlFlow.Stage,
                    Toolbox.ControlFlow.ControlFlow.CurrentLoadProcess?.Id
                );
            if (!DisableLogging)
                Logger.Info<DataFlowTask>(
                    TaskName,
                    TaskType,
                    "END",
                    TaskHash,
                    Toolbox.ControlFlow.ControlFlow.Stage,
                    Toolbox.ControlFlow.ControlFlow.CurrentLoadProcess?.Id
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

            Logger.Info<DataFlowTask>(
                TaskName + $" processed {ProgressCount} records.",
                TaskType,
                "LOG",
                TaskHash,
                Toolbox.ControlFlow.ControlFlow.Stage,
                Toolbox.ControlFlow.ControlFlow.CurrentLoadProcess?.Id
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

            Logger.Info<DataFlowTask>(
                TaskName + $" processed {ProgressCount} records.",
                TaskType,
                "LOG",
                TaskHash,
                Toolbox.ControlFlow.ControlFlow.Stage,
                Toolbox.ControlFlow.ControlFlow.CurrentLoadProcess?.Id
            );
        }
    }
}
