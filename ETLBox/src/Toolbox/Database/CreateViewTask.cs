using ETLBox.ConnectionManager;

namespace ETLBox.ControlFlow
{
    /// <summary>
    /// Creates or updates a view.
    /// </summary>
    public class CreateViewTask : GenericTask, ITask
    {
        public override string TaskName => $"{CreateOrAlterSql} VIEW {ViewName}";
        public void Execute()
        {
            IsExisting = new IfTableOrViewExistsTask(ViewName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if ((ConnectionType == ConnectionManagerType.SQLite
                || ConnectionType == ConnectionManagerType.Postgres
                || ConnectionType == ConnectionManagerType.Access
                )
                && IsExisting)
                new DropViewTask(ViewName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Drop();
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        public string ViewName { get; set; }
        public ObjectNameDescriptor VN => new ObjectNameDescriptor(ViewName, QB, QE);
        string CreateViewName => ConnectionType == ConnectionManagerType.Access ? VN.UnquotatedFullName : VN.QuotatedFullName;
        public string Definition { get; set; }
        public string Sql
        {
            get
            {
                return $@"{CreateOrAlterSql} VIEW {CreateViewName}
AS
{Definition}
";
            }
        }
        public CreateViewTask()
        {

        }
        public CreateViewTask(string viewName, string definition) : this()
        {
            this.ViewName = viewName;
            this.Definition = definition;
        }

        public static void CreateOrAlter(string viewName, string definition) => new CreateViewTask(viewName, definition).Execute();
        public static void CreateOrAlter(IConnectionManager connectionManager, string viewName, string definition) => new CreateViewTask(viewName, definition) { ConnectionManager = connectionManager }.Execute();

        bool IsExisting { get; set; }
        string CreateOrAlterSql => IsExisting &&
            (ConnectionType != ConnectionManagerType.SQLite && ConnectionType != ConnectionManagerType.Postgres) ? "ALTER" : "CREATE";
    }
}
