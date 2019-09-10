using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow {
    /// <summary>
    /// Creates or updates a view.
    /// </summary>
    public class CreateOrAlterViewTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CRUDVIEW";
        public override string TaskName => $"{CreateOrAlterSql} VIEW {ViewName}";
        public override void Execute() {
            IsExisting = new IfExistsTask(ViewName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string ViewName { get; set; }
        public string Definition { get; set; }
        public string Sql => $@"{CreateOrAlterSql} VIEW {ViewName}
AS
{Definition}
";

        public CreateOrAlterViewTask() {

        }
        public CreateOrAlterViewTask(string viewName, string definition) : this() {
            this.ViewName = viewName;
            this.Definition = definition;
        }

        public static void CreateOrAlter(string viewName, string definition) => new CreateOrAlterViewTask(viewName, definition).Execute();
        public static void CreateOrAlter(IConnectionManager connectionManager, string viewName, string definition) => new CreateOrAlterViewTask(viewName, definition) { ConnectionManager = connectionManager }.Execute();

        bool IsExisting { get; set; }
        string CreateOrAlterSql => IsExisting ? "ALTER" : "CREATE";

    }
}
