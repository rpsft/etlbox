using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.Helper;
using ALE.ETLBox.ControlFlow;
using System;

namespace ALE.ETLBox {
    public abstract class GenericTask : ITask {
        public virtual string TaskType { get; set; } = "N/A";
        public virtual string TaskName { get; set; } = "N/A";
        public virtual void Execute() {
            throw new Exception("Not implemented!");
        }

        public virtual IConnectionManager ConnectionManager { get; set; }

        internal virtual IDbConnectionManager DbConnectionManager {
            get {
                if (ConnectionManager == null)
                    return (IDbConnectionManager)ControlFlow.ControlFlow.CurrentDbConnection;
               else
                    return (IDbConnectionManager)ConnectionManager;
            }
        }

        public bool _disableLogging;
        public virtual bool DisableLogging {
            get {
                if (ControlFlow.ControlFlow.DisableAllLogging == false)
                    return _disableLogging;
                else
                    return ControlFlow.ControlFlow.DisableAllLogging;
            }
            set {
                _disableLogging = value;
            }
        }

        private string _taskHash;
        public virtual string TaskHash {
            get {
                if (_taskHash == null)
                    return HashHelper.Encrypt_Char40(this);
                else
                    return _taskHash;
            }
            set {
                _taskHash = value;
            }
        }
        internal virtual bool HasName => !String.IsNullOrWhiteSpace(TaskName);
    }
}
