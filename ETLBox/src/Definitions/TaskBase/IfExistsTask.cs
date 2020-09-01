using ETLBox.ControlFlow.Tasks;
using ETLBox.Helper;

namespace ETLBox.ControlFlow
{
    public abstract class IfExistsTask : ControlFlowTask
    {
        public override string TaskName => $"Check if {ObjectName} exists";
        public void Execute()
        {
            if (Sql != string.Empty)
                DoesExist = new SqlTask(this, Sql).ExecuteScalarAsBool();
        }

        public string ObjectName { get; set; }
        public ObjectNameDescriptor ON => new ObjectNameDescriptor(ObjectName, QB, QE);
        internal string OnObjectName { get; set; }
        public ObjectNameDescriptor OON => new ObjectNameDescriptor(OnObjectName, QB, QE);
        public bool DoesExist { get; internal set; }

        public string Sql
        {
            get
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