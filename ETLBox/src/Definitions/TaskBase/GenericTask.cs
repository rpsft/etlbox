using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.Helper;
using System;
using System.Globalization;
using CF = ALE.ETLBox.ControlFlow;

namespace ALE.ETLBox
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

        internal virtual IConnectionManager DbConnectionManager =>
            ConnectionManager == null
                ? (IConnectionManager)ControlFlow.ControlFlow.DefaultDbConnection
                : (IConnectionManager)ConnectionManager;

        public ConnectionManagerType ConnectionType => this.DbConnectionManager.ConnectionManagerType;
        public string QB => DbConnectionManager.QB;
        public string QE => DbConnectionManager.QE;

        public bool _disableLogging;
        public virtual bool DisableLogging
        {
            get => ControlFlow.ControlFlow.DisableAllLogging == false 
                ? _disableLogging 
                : ControlFlow.ControlFlow.DisableAllLogging;
            set => _disableLogging = value;
        }

        public virtual CultureInfo CurrentCulture => ConnectionManager?.ConnectionCulture;

        private string _taskHash;


        public virtual string TaskHash
        {
            get => _taskHash ?? HashHelper.Encrypt_Char40(this);
            set => _taskHash = value;
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
