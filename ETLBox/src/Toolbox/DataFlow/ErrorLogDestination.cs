using System.Collections.Concurrent;
using ALE.ETLBox.Common.DataFlow;
using ETLBox.Primitives;
using Microsoft.Extensions.Logging;

namespace ALE.ETLBox.DataFlow
{
    public class ErrorLogDestination : DataFlowDestination<ETLBoxError>
    {
        /* ITask Interface */
        public override string TaskName => "Write error";

        public BlockingCollection<ETLBoxError> Errors { get; set; } = new();

        public ErrorLogDestination()
        {
            TargetAction = new ActionBlock<ETLBoxError>(WriteRecord);
            SetCompletionTask();
        }

        /// <summary>
        /// Creates a new instance with an injected logger.
        /// </summary>
        public ErrorLogDestination(ILogger<ErrorLogDestination> logger)
            : base(logger)
        {
            TargetAction = new ActionBlock<ETLBoxError>(WriteRecord);
            SetCompletionTask();
        }

        private void WriteRecord(ETLBoxError error)
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
            var logException = LoggerMessage.Define<string, string>(
                LogLevel.Error,
                0,
                "{ErrorText}: {RecordAsJson}"
            );
            logException.Invoke(Logger, error.ErrorText, error.RecordAsJson, error.Exception);
        }

        protected override void CleanUp()
        {
            Errors?.CompleteAdding();
            OnCompletion?.Invoke();
            LogFinish();
        }
    }
}
