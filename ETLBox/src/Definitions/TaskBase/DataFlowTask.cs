namespace ALE.ETLBox
{
    public abstract class DataFlowTask : GenericTask, ITask
    {
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

        public virtual int ProgressCount { get; set; }

        public bool HasLoggingThresholdRows => LoggingThresholdRows != null && LoggingThresholdRows > 0;
    }
}
