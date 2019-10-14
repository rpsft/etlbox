namespace ALE.ETLBox.ControlFlow
{
    public abstract class DropTask<T> : GenericTask, ITask where T : IfExistsTask, new()
    {
        public override string TaskName => $"Drop Object {ObjectName}";
        public void Execute()
        {
            bool objectExists = new T() { ObjectName = ObjectName, OnObjectName = OnObjectName, ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (objectExists)
                new SqlTask(this, Sql).ExecuteNonQuery();
        }

        public string ObjectName { get; set; }
        public TableNameDescriptor ON => new TableNameDescriptor(ObjectName, ConnectionType);
        internal string OnObjectName { get; set; }
        public string Sql => GetSql();
        internal virtual string GetSql() => string.Empty;
        public void Drop() => new SqlTask(this, Sql).ExecuteNonQuery();
        public void DropIfExists() => Execute();
    }
}
