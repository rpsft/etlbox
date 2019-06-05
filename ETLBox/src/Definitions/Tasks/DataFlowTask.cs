using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.Helper;
using ALE.ETLBox.ControlFlow;
using System;
using ALE.ETLBox.Logging;

namespace ALE.ETLBox {
    public abstract class DataFlowTask : GenericTask, ITask {
        public int? _loggingThresholdRows;
        public virtual int? LoggingThresholdRows
        {
            get
            {
                if (DataFlow.DataFlow.HasLoggingThresholdRows)
                    return DataFlow.DataFlow.LoggingThresholdRows;
                else
                    return _loggingThresholdRows;
            }
            set
            {
                _loggingThresholdRows = value;
            }
        }

        private IDbConnectionManager _sourceDbConnection;
        private IDbConnectionManager _destinationDbConnection;

        public IDbConnectionManager SourceDbConnection
        {
            get {
                if (_sourceDbConnection != null)
                    return _sourceDbConnection;
                else if (this.ConnectionManager != null)
                    return (IDbConnectionManager)ConnectionManager;
                else
                    return DataFlow.DataFlow.SourceDbConnection;
            }
            set => _sourceDbConnection = value;
        }
        public IDbConnectionManager DestinationDbConnection
        {
            get
            {
                if (_destinationDbConnection != null)
                    return _destinationDbConnection;
                else if (this.ConnectionManager != null)
                    return (IDbConnectionManager)ConnectionManager;
                else
                    return DataFlow.DataFlow.DestinationDbConnection;
            }
            set => _destinationDbConnection = value;
        }

        public virtual int ProgressCount { get; set; }

        public bool HasLoggingThresholdRows => LoggingThresholdRows != null && LoggingThresholdRows > 0;
    }
}
