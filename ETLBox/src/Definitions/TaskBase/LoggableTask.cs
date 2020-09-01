using ETLBox.Connection;
using ETLBox.Helper;
using System;

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
        public virtual string TaskType
        {
            get => String.IsNullOrEmpty(_taskType) ? this.GetType().Name : _taskType;
            set => _taskType = value;
        }

        /// <summary>
        /// A name to identify the task or component. Every component or task comes
        /// with a default name that can be overwritten.
        /// </summary>
        public virtual string TaskName { get; set; } = "N/A";

        internal NLog.Logger NLogger { get; set; } = Logging.Logging.GetLogger();

        public bool _disableLogging;

        /// <summary>
        /// If set to true, the component or task won't produce any log output.
        /// </summary>
        public virtual bool DisableLogging
        {
            get
            {
                if (Logging.Logging.DisableAllLogging == false)
                    return _disableLogging;
                else
                    return Logging.Logging.DisableAllLogging;
            }
            set
            {
                _disableLogging = value;
            }
        }

        private string _taskHash;

        /// <summary>
        /// Creates a unique hash value to identify the task.
        /// </summary>
        public virtual string TaskHash
        {
            get
            {
                if (_taskHash == null)
                    return HashHelper.CreateChar40Hash(this);
                else
                    return _taskHash;
            }
            set
            {
                _taskHash = value;
            }
        }
        internal virtual bool HasName => !String.IsNullOrWhiteSpace(TaskName);

        public LoggableTask()
        { }

        public void CopyLogTaskProperties(ILoggableTask otherTask)
        {
            this.TaskName = otherTask.TaskName;
            this.TaskHash = otherTask.TaskHash;
            this.TaskType = otherTask.TaskType;
            if (this.DisableLogging == false)
                this.DisableLogging = otherTask.DisableLogging;
        }
    }
}
