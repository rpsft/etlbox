using ETLBox.Helper;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ETLBox.ControlFlow
{
    /// <summary>
    /// This class contains properties that are needed for logging.
    /// </summary>
    public abstract class LoggableTask : ILoggableTask
    {
        private string _taskType;

        /// <summary>
        /// A type description of the task or component. This is usually the class name.
        /// </summary>
        public virtual string TaskType {
            get {
                if (String.IsNullOrEmpty(_taskType)) {
                    var type = this.GetType();
                    string name = type.Name;
                    List<string> argnames = new List<string>();
                    foreach (var arg in type.GetGenericArguments())
                        argnames.Add(arg.Name);
                    if (argnames.Count > 0)
                        return $"{name}<{string.Join(",", argnames)}>";
                    else
                        return name;
                } else
                    return _taskType;
            }
            set => _taskType = value;
        }

        /// <summary>
        /// A name to identify the task or component. Every component or task comes
        /// with a default name that can be overwritten.
        /// </summary>
        public virtual string TaskName { get; set; } = "N/A";

        /// <summary>
        /// If set to true, the component or task won't produce any log output.
        /// </summary>
        public virtual bool DisableLogging {
            get {
                if (Logging.Logging.DisableAllLogging == false)
                    return _disableLogging;
                else
                    return Logging.Logging.DisableAllLogging;
            }
            set {
                _disableLogging = value;
            }
        }
        private bool _disableLogging;

        /// <summary>
        /// Creates a unique hash value to identify the task.
        /// </summary>
        public virtual string TaskHash {
            get {
                if (_taskHash == null)
                    return HashHelper.CreateChar40Hash(this);
                else
                    return _taskHash;
            }
            set {
                _taskHash = value;
            }
        }

        private string _taskHash;

        internal virtual bool HasName => !String.IsNullOrWhiteSpace(TaskName);

        public LoggableTask() { }

        /// <summary>
        /// Copies the relevant task properties from the current loggable task
        /// to another loggable task.
        /// </summary>
        /// <param name="otherTask">The target task that retrieve a copy from the log task properties</param>
        public void CopyLogTaskProperties(ILoggableTask otherTask) {
            this.TaskName = otherTask.TaskName;
            this.TaskHash = otherTask.TaskHash;
            this.TaskType = otherTask.TaskType;
            if (this.DisableLogging == false)
                this.DisableLogging = otherTask.DisableLogging;
        }

        internal void LogInfo(string message, params object[] args) {
            if (DisableLogging) return;
            using (ETLBox.Logging.Logging.LogInstance?.BeginScope(CreateScopeDict())) {
                ETLBox.Logging.Logging.LogInstance?.LogInformation(message, args);
            }
        }

        private KeyValuePair<string, object>[] CreateScopeDict() {
            return new[] {
                    new KeyValuePair<string, object>("taskName", this.TaskName),
                    new KeyValuePair<string, object>("taskHash", this.TaskHash),
                    new KeyValuePair<string, object>("taskType", this.TaskType),
                    new KeyValuePair<string, object>("stage", Logging.Logging.STAGE),
                    new KeyValuePair<string, object>("loadProcessId", Logging.Logging.CurrentLoadProcess?.Id)
                };
        }


        internal void LogDebug(string message, params object[] args) {
            if (DisableLogging) return;
            using (ETLBox.Logging.Logging.LogInstance?.BeginScope(CreateScopeDict())) {
                ETLBox.Logging.Logging.LogInstance?.LogDebug(message, args);
            }
        }

        internal void LogTrace(string message, params object[] args) {
            //Ignore disable flag, be verbose!
            using (ETLBox.Logging.Logging.LogInstance?.BeginScope(CreateScopeDict())) {
                ETLBox.Logging.Logging.LogInstance?.LogTrace(message, args);
            }
        }

        internal void LogWarn(string message, params object[] args) {
            if (DisableLogging) return;
            using (ETLBox.Logging.Logging.LogInstance?.BeginScope(CreateScopeDict())) {
                ETLBox.Logging.Logging.LogInstance?.LogWarning(message, args);
            }
        }

        internal void LogError(string message, params object[] args) {
            if (DisableLogging) return;
            using (ETLBox.Logging.Logging.LogInstance?.BeginScope(CreateScopeDict())) {
                ETLBox.Logging.Logging.LogInstance?.LogError(message, args);
            }
        }

        internal void LogFatal(string message, params object[] args) {
            using (ETLBox.Logging.Logging.LogInstance?.BeginScope(CreateScopeDict())) {
                ETLBox.Logging.Logging.LogInstance?.LogCritical(message, args);
            }
        }
    }
}
