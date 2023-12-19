using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace ALE.ETLBox.DataFlow
{
    public class ErrorLogDestination: DataFlowDestination<ETLBoxError>
    {
        /* ITask Interface */
        public override string TaskName => "Write error into memory";

        public BlockingCollection<ETLBoxError> Errors { get; set; } = new();

        public ErrorLogDestination()
        {
            TargetAction = new ActionBlock<ETLBoxError>(WriteRecord);
            SetCompletionTask();
        }

        internal ErrorLogDestination(ITask callingTask)
            : this()
        {
            CopyTaskProperties(callingTask);
        }

        public void WriteRecord(ETLBoxError error)
        {
            Errors ??= new BlockingCollection<ETLBoxError>();
            if (error is null)
                return;
            Errors.Add(error);

            if (
                DisableLogging
                || !HasLoggingThresholdRows
                || ProgressCount % LoggingThresholdRows != 0
            )
            {
                return;
            }

            Logger.LogError(error.Exception, $"{error.ErrorText}: {error.RecordAsJson}");
        }

        protected override void CleanUp()
        {
            Errors?.CompleteAdding();
            OnCompletion?.Invoke();
            LogFinish();
        }
    }
}
