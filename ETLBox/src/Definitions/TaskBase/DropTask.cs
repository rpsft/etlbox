using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Drops an object- abstract implementation.
    /// </summary>
    public abstract class DropTask<T> : GenericTask, ITask where T : IfExistsTask, new()
    {
        /* ITask Interface */
        public override string TaskName => $"Drop Object {ObjectName}";
        public override void Execute()
        {
            bool objectExists = new T() { ObjectName = ObjectName, OnObjectName = OnObjectName, ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (objectExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string ObjectName { get; set; }
        public TableNameDescriptor ON => new TableNameDescriptor(ObjectName, ConnectionType);
        internal string OnObjectName { get; set; }
        public string Sql => GetSql();
        internal virtual string GetSql() => string.Empty;
        public void Drop() => new SqlTask(this, Sql).ExecuteNonQuery();
        public void DropIfExists() => Execute();
    }
}
