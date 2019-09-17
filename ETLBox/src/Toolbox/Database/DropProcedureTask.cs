using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Drops a procedure if the procedure exists.
    /// </summary>
    public class DropProcedureTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DROPPROC";
        public override string TaskName => $"Drop Procedure {ProcedureName}";
        public override void Execute()
        {
            if (ConnectionType == ConnectionManagerType.SQLite)
                throw new ETLBoxNotSupportedException("This task is not supported with SQLite!");

            bool procExists = new IfExistsTask(ProcedureName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (procExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string ProcedureName { get; set; }
        public string Sql
        {
            get
            {
                return $@"DROP PROCEDURE {ProcedureName}";
            }
        }

        public void Drop() => Execute();

        /* Some constructors */
        public DropProcedureTask()
        {
        }

        public DropProcedureTask(string procedureName) : this()
        {
            ProcedureName = procedureName;
        }


        /* Static methods for convenience */
        public static void Drop(string procedureName) => new DropProcedureTask(procedureName).Execute();
        public static void Drop(IConnectionManager connectionManager, string procedureName) => new DropProcedureTask(procedureName) { ConnectionManager = connectionManager }.Execute();


    }


}
