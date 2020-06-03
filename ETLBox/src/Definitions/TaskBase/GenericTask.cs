using ETLBox.Connection;
using ETLBox.Helper;
using System;
using CF = ETLBox.ControlFlow;

namespace ETLBox
{
    public abstract class GenericTask : ITask
    {
        private string _taskType;
        public virtual string TaskType
        {
            get => String.IsNullOrEmpty(_taskType) ? this.GetType().Name : _taskType;
            set => _taskType = value;
        }
        public virtual string TaskName { get; set; } = "N/A";
        public NLog.Logger NLogger { get; set; } = CF.ControlFlow.GetLogger();

        public virtual IConnectionManager ConnectionManager { get; set; }

        internal virtual IConnectionManager DbConnectionManager
        {
            get
            {
                if (ConnectionManager == null)
                    return (IConnectionManager)ControlFlow.ControlFlow.DefaultDbConnection;
                else
                    return (IConnectionManager)ConnectionManager;
            }
        }

        public ConnectionManagerType ConnectionType => this.DbConnectionManager.ConnectionManagerType;
        public string QB => DbConnectionManager.QB;
        public string QE => DbConnectionManager.QE;

        public bool _disableLogging;
        public virtual bool DisableLogging
        {
            get
            {
                if (ControlFlow.ControlFlow.DisableAllLogging == false)
                    return _disableLogging;
                else
                    return ControlFlow.ControlFlow.DisableAllLogging;
            }
            set
            {
                _disableLogging = value;
            }
        }

        private string _taskHash;


        public virtual string TaskHash
        {
            get
            {
                if (_taskHash == null)
                    return HashHelper.Encrypt_Char40(this);
                else
                    return _taskHash;
            }
            set
            {
                _taskHash = value;
            }
        }
        internal virtual bool HasName => !String.IsNullOrWhiteSpace(TaskName);

        public GenericTask()
        { }

        public void CopyTaskProperties(ITask otherTask)
        {
            this.TaskName = otherTask.TaskName;
            this.TaskHash = otherTask.TaskHash;
            this.TaskType = otherTask.TaskType;
            this.ConnectionManager = otherTask.ConnectionManager;
            this.DisableLogging = otherTask.DisableLogging;
        }
    }
}
