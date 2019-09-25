using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;

namespace ALE.ETLBox.Logging
{
    /// <summary>
    /// Sets the TransferCompletedDate for the current load process.
    /// </summary>
    public class TransferCompletedForLoadProcessTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Set transfer completed for {LoadProcessKey}";
        public override void Execute()
        {
            new SqlTask(this, Sql).ExecuteNonQuery();
            var rlp = new ReadLoadProcessTableTask(LoadProcessKey)
            {
                TaskType = this.TaskType,
                TaskHash = this.TaskHash,
                DisableLogging = true,
                ConnectionManager = this.ConnectionManager
            };
            rlp.Execute();
            ControlFlow.ControlFlow.CurrentLoadProcess = rlp.LoadProcess;
        }

        /* Public properties */
        public int? _loadProcessKey;
        public int? LoadProcessKey
        {
            get
            {
                return _loadProcessKey ?? ControlFlow.ControlFlow.CurrentLoadProcess?.LoadProcessKey;
            }
            set
            {
                _loadProcessKey = value;
            }
        }

        public string Sql => $@"EXECUTE etl.TransferCompletedForLoadProcess
	 @LoadProcessKey = '{LoadProcessKey}'";

        public TransferCompletedForLoadProcessTask()
        {

        }

        public TransferCompletedForLoadProcessTask(int? loadProcessKey) : this()
        {
            this.LoadProcessKey = loadProcessKey;
        }

        public static void Complete() => new TransferCompletedForLoadProcessTask().Execute();
        public static void Complete(int? loadProcessKey) => new TransferCompletedForLoadProcessTask(loadProcessKey).Execute();
        public static void Complete(IConnectionManager connectionManager)
            => new TransferCompletedForLoadProcessTask() { ConnectionManager = connectionManager }.Execute();
        public static void Complete(IConnectionManager connectionManager, int? loadProcessKey)
            => new TransferCompletedForLoadProcessTask(loadProcessKey) { ConnectionManager = connectionManager }.Execute();


    }
}
