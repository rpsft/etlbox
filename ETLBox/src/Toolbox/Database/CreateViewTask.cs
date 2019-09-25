using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Creates or updates a view.
    /// </summary>
    public class CreateViewTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskName => $"{CreateOrAlterSql} VIEW {ViewName}";
        public override void Execute()
        {
            IsExisting = new IfTableExistsTask(ViewName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (ConnectionType == ConnectionManagerType.SQLite && IsExisting)
                new DropViewTask(ViewName) { ConnectionManager = this.ConnectionManager, DisableLogging = true}.Drop();
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string ViewName { get; set; }
        public string Definition { get; set; }
        public string Sql
        {
            get
            {
                    return $@"{CreateOrAlterSql} VIEW {ViewName}
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
        string CreateOrAlterSql => IsExisting && ConnectionType != ConnectionManagerType.SQLite ? "ALTER" : "CREATE";
    }
}
