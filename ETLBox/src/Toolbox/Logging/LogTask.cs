using ETLBox.Connection;
using ETLBox.ControlFlow;

namespace ETLBox.Logging
{
    /// <summary>
    /// Used this task for custom log messages.
    /// </summary>
    public class LogTask : ControlFlowTask
    {
        /* ITask Interface */
        public override string TaskName => $"Logs message";
        public void Execute()
        {
            Info(Message);
        }

        /* Public properties */
        public string Message { get; set; }

        public LogTask()
        {
        }

        public LogTask(string message) : this()
        {
            Message = message;
        }
        //NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        public void Trace() => NLogger?.Trace(Message, TaskType, "LOG", TaskHash, Logging.STAGE, Logging.CurrentLoadProcess?.Id);
        public void Debug() => NLogger?.Debug(Message, TaskType, "LOG", TaskHash, Logging.STAGE, Logging.CurrentLoadProcess?.Id);
        public void Info() => NLogger?.Info(Message, TaskType, "LOG", TaskHash, Logging.STAGE, Logging.CurrentLoadProcess?.Id);
        public void Warn() => NLogger?.Warn(Message, TaskType, "LOG", TaskHash, Logging.STAGE, Logging.CurrentLoadProcess?.Id);
        public void Error() => NLogger?.Error(Message, TaskType, "LOG", TaskHash, Logging.STAGE, Logging.CurrentLoadProcess?.Id);
        public void Fatal() => NLogger?.Fatal(Message, TaskType, "LOG", TaskHash, Logging.STAGE, Logging.CurrentLoadProcess?.Id);
        public static void Trace(string message) => new LogTask(message).Trace();
        public static void Debug(string message) => new LogTask(message).Debug();
        public static void Info(string message) => new LogTask(message).Info();
        public static void Warn(string message) => new LogTask(message).Warn();
        public static void Error(string message) => new LogTask(message).Error();
        public static void Fatal(string message) => new LogTask(message).Fatal();
        public static void Trace(IConnectionManager connectionManager, string message) => new LogTask(message) { ConnectionManager = connectionManager }.Trace();
        public static void Debug(IConnectionManager connectionManager, string message) => new LogTask(message) { ConnectionManager = connectionManager }.Debug();
        public static void Info(IConnectionManager connectionManager, string message) => new LogTask(message) { ConnectionManager = connectionManager }.Info();
        public static void Warn(IConnectionManager connectionManager, string message) => new LogTask(message) { ConnectionManager = connectionManager }.Warn();
        public static void Error(IConnectionManager connectionManager, string message) => new LogTask(message) { ConnectionManager = connectionManager }.Error();
        public static void Fatal(IConnectionManager connectionManager, string message) => new LogTask(message) { ConnectionManager = connectionManager }.Fatal();
    }
}
