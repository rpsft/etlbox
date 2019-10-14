using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;

namespace ALE.ETLBox.Logging
{
    /// <summary>
    /// Removes log data older than the specified days to keep.
    /// </summary>
    public class CleanUpLogTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"Clean up log tables";
        public void Execute()
        {
            new SqlTask(this, Sql) { DisableLogging = true, DisableExtension = true }.ExecuteNonQuery();
        }

        public int DaysToKeep { get; set; }

        /* Public properties */
        public string Sql => $@"
DELETE FROM etl.Log
 WHERE LogDate < Dateadd(day,-{DaysToKeep},GETDATE())
";

        public CleanUpLogTask() { }

        public CleanUpLogTask(int daysToKeep) : this()
        {
            DaysToKeep = daysToKeep;
        }
        public static void CleanUp(int daysToKeep) => new CleanUpLogTask(daysToKeep).Execute();
        public static void CleanUp(IConnectionManager connectionManager, int daysToKeep)
            => new CleanUpLogTask(daysToKeep) { ConnectionManager = connectionManager }.Execute();




    }
}
