using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using ETLBox.Primitives;
using ALE.ETLBox.Common.DataFlow;

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

        private void WriteRecord(ETLBoxError error)
        {
            Errors ??= new BlockingCollection<ETLBoxError>();
            if (error is null)
                return;
            Errors.Add(error);

            if (DisableLogging)
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
