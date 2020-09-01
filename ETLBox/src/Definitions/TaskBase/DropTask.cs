using ETLBox.ControlFlow.Tasks;
using ETLBox.Helper;

namespace ETLBox.ControlFlow
{
    public abstract class DropTask<T> : ControlFlowTask where T : IfExistsTask, new()
    {
        public override string TaskName => $"Drop Object {ObjectName}";
        public void Execute()
        {
            bool objectExists = new T() { ObjectName = ObjectName, OnObjectName = OnObjectName, ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (objectExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        public string ObjectName { get; set; }
        public ObjectNameDescriptor ON => new ObjectNameDescriptor(ObjectName, QB, QE);
        internal string OnObjectName { get; set; }
        public string Sql => GetSql();
        internal virtual string GetSql() => string.Empty;
        public void Drop() => new SqlTask(this, Sql).ExecuteNonQuery();
        public void DropIfExists() => Execute();
    }
}
