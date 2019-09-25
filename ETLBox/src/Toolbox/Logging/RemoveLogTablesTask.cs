using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;

namespace ALE.ETLBox.Logging {
    /// <summary>
    /// Removes the log tables and all the log procedures.
    /// </summary>
    public class RemoveLogTablesTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskName => $"Remove log tables";
        public override void Execute() => new SqlTask(this, Sql).ExecuteNonQuery();
        public string Sql => $@"
if (object_id('etl.Log')  is not null) drop table etl.Log
if (object_id('etl.LoadProcess')  is not null) drop table etl.LoadProcess
if (object_id('etl.AbortLoadProcess')  is not null) drop procedure etl.AbortLoadProcess
if (object_id('etl.EndLoadProcess')  is not null) drop procedure etl.EndLoadProcess
if (object_id('etl.StartLoadProcess')  is not null) drop procedure etl.StartLoadProcess
if (object_id('etl.TransferCompletedForLoadProcess')  is not null) drop procedure etl.TransferCompletedForLoadProcess
";

        public RemoveLogTablesTask() { }
        public RemoveLogTablesTask(IConnectionManager connectionManager) {
            this.ConnectionManager = connectionManager;
        }

        public static void Remove() => new RemoveLogTablesTask().Execute();

        public static void Remove(IConnectionManager connectionManager) => new RemoveLogTablesTask(connectionManager).Execute();


    }
}
