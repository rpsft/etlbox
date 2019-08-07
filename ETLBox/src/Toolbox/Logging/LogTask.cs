namespace ALE.ETLBox.Logging {
    /// <summary>
    /// Used this task for custom log messages.
    /// </summary>
    public class LogTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "LOG";
        public override string TaskName => $"Logs message";
        public override void Execute() {
            Info(Message);
        }

        /* Public properties */
        public string Message { get; set; }

        public LogTask() {
        }

        public LogTask(string message) : this() {
            Message = message; 
        }
        //NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.STAGE, ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        public void Trace() => NLogger?.Trace(Message, TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        public void Debug() => NLogger?.Debug(Message, TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        public void Info() => NLogger?.Info(Message, TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        public void Warn() => NLogger?.Warn(Message, TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        public void Error() => NLogger?.Error(Message, TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        public void Fatal() => NLogger?.Fatal(Message, TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        public static void Trace(string message) => new LogTask(message).Trace();
        public static void Debug(string message) => new LogTask(message).Debug();
        public static void Info(string message) => new LogTask(message).Info();
        public static void Warn(string message) => new LogTask(message).Warn();
        public static void Error(string message) => new LogTask(message).Error();
        public static void Fatal(string message) => new LogTask(message).Fatal();
    }
}
