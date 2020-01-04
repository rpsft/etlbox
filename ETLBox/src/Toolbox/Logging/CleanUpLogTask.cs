using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using System;
using System.Collections.Generic;

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
            QueryParameter par = new QueryParameter("DeleteAfter", "DATETIME", DeleteAfter);
            new SqlTask(this, Sql)
            {
                Parameter = new List<QueryParameter>() { par },
                DisableLogging = true,
            }.ExecuteNonQuery();
        }

        public int DaysToKeep { get; set; }
        public DateTime DeleteAfter => new DateTime(DateTime.Now.Year
                                , DateTime.Now.Month, DateTime.Now.Day).AddDays((DaysToKeep * -1));

        /* Public properties */
        public string Sql => $@"
DELETE FROM {TN.QuotatedFullName} WHERE {QB}log_date{QE} < @DeleteAfter
";

        ObjectNameDescriptor TN => new ObjectNameDescriptor(ControlFlow.ControlFlow.LogTable, this.ConnectionType);

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
