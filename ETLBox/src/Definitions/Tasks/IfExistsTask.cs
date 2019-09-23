using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Abstract base class to check if an object exists.
    /// </summary>
    public abstract class IfExistsTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "IFEXISTS";
        public override string TaskName => $"Check if {ObjectName} exists";
        public override void Execute()
        {
            if (Sql != string.Empty)
                DoesExist = new SqlTask(this, Sql).ExecuteScalarAsBool();
        }

        /* Public properties */
        public string ObjectName { get; set; }
        public bool DoesExist { get; private set; }

        public string Sql { get
            {
                return GetSql();
            }
        }

        internal virtual string GetSql()
        {
            return string.Empty;
        }

        public virtual bool Exists()
        {
            Execute();
            return DoesExist;
        }
    }
}