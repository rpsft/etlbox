using ETLBox.Connection;
using ETLBox.Helper;
using System;

namespace ETLBox.ControlFlow
{
    public abstract class ControlFlowTask : LoggableTask
    {
        public virtual IConnectionManager ConnectionManager { get; set; }

        internal virtual IConnectionManager DbConnectionManager
        {
            get
            {
                if (ConnectionManager == null)
                    return (IConnectionManager)ControlFlow.DefaultDbConnection;
                else
                    return (IConnectionManager)ConnectionManager;
            }
        }

        public ConnectionManagerType ConnectionType => this.DbConnectionManager.ConnectionManagerType;
        public string QB => DbConnectionManager.QB;
        public string QE => DbConnectionManager.QE;
    }
}
