using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Helper;
using Microsoft.Extensions.Logging;

namespace ALE.ETLBox.src.Definitions.TaskBase
{
    [PublicAPI]
    public abstract class GenericTask : ITask
    {
        private string _taskType;
        public virtual string TaskType
        {
            get => string.IsNullOrEmpty(_taskType) ? GetType().Name : _taskType;
            set => _taskType = value;
        }
        public virtual string TaskName { get; set; } = "N/A";

        public ILogger Logger { get; set; } = Toolbox.ControlFlow.ControlFlow.LoggerFactory.CreateLogger<GenericTask>();

        public IConnectionManager ConnectionManager
        {
            get => _connectionManager;
            set
            {
                _connectionManager = value;
                OnConnectionManagerChanged(value);
            }
        }

        protected virtual void OnConnectionManagerChanged(IConnectionManager value) { }

        internal virtual IConnectionManager DbConnectionManager =>
            ConnectionManager ?? Toolbox.ControlFlow.ControlFlow.DefaultDbConnection;

        public ConnectionManagerType ConnectionType => DbConnectionManager.ConnectionManagerType;
        public string QB => DbConnectionManager.QB;
        public string QE => DbConnectionManager.QE;

        private bool _disableLogging;
        public virtual bool DisableLogging
        {
            get => Toolbox.ControlFlow.ControlFlow.DisableAllLogging || _disableLogging;
            set => _disableLogging = value;
        }

        public virtual CultureInfo CurrentCulture => ConnectionManager?.ConnectionCulture;

        private string _taskHash;
        private IConnectionManager _connectionManager;

        public virtual string TaskHash
        {
            get => _taskHash ?? HashHelper.Encrypt_Char40(this);
            set => _taskHash = value;
        }
        internal virtual bool HasName => !string.IsNullOrWhiteSpace(TaskName);

        public void CopyTaskProperties(ITask otherTask)
        {
            TaskName = otherTask.TaskName;
            TaskHash = otherTask.TaskHash;
            TaskType = otherTask.TaskType;
            ConnectionManager = otherTask.ConnectionManager;
            DisableLogging = otherTask.DisableLogging;
        }
    }
}
