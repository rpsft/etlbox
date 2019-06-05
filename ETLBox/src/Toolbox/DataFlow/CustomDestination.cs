using System;
using System.Threading.Tasks.Dataflow;

namespace ALE.ETLBox.DataFlow {
    /// <summary>
    /// Define your own destination block.
    /// </summary>
    /// <typeparam name="TInput">Type of datasoure input.</typeparam>
    public class CustomDestination<TInput> : DataFlowTask, ITask, IDataFlowDestination<TInput> {

        /* ITask Interface */
        public override string TaskType { get; set; } = "DF_CUSTOMDEST";
        public override string TaskName => $"Dataflow: Write Data into custom target";
        public override void Execute() { throw new Exception("Dataflow destinations can't be started directly"); }

        /* Public properties */
        public ITargetBlock<TInput> TargetBlock => TargetActionBlock;
        public Action<TInput> WriteAction {
            get {
                return _writeAction;
            }
            set {
                _writeAction = value;
                TargetActionBlock = new ActionBlock<TInput>(_writeAction);

            }
        }

        /* Private stuff */
        private Action<TInput> _writeAction;

        internal ActionBlock<TInput> TargetActionBlock { get; set; }

        NLog.Logger NLogger { get; set; }
        public CustomDestination() {
            NLogger = NLog.LogManager.GetLogger("ETL");
        }

        public CustomDestination(Action<TInput> writeAction) : this()
        {
            if (ProgressCount == 0) NLogStart();
            WriteAction = writeAction;
            LogProgress(1);
        }

        public void Wait()  {
            TargetActionBlock.Completion.Wait();
            NLogFinish();
        }

        void NLogStart()
        {
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "START", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void NLogFinish()
        {
            if (!DisableLogging && HasLoggingThresholdRows)
                NLogger.Info(TaskName + $" processed {ProgressCount} records in total.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
            if (!DisableLogging)
                NLogger.Info(TaskName, TaskType, "END", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }

        void LogProgress(int rowsProcessed)
        {
            ProgressCount += rowsProcessed;
            if (!DisableLogging && HasLoggingThresholdRows && (ProgressCount % LoggingThresholdRows == 0))
                NLogger.Info(TaskName + $" processed {ProgressCount} records.", TaskType, "LOG", TaskHash, ControlFlow.ControlFlow.STAGE, ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey);
        }
    }

}
