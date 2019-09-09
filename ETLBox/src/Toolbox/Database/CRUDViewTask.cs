using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow {
    /// <summary>
    /// Creates or updates a view.
    /// </summary>
    public class CRUDViewTask : GenericTask, ITask {
        /* ITask Interface */
        public override string TaskType { get; set; } = "CRUDVIEW";
        public override string TaskName => $"{CreateOrAlterSql} VIEW {ViewName}";
        public override void Execute() {
            IsExisting = new SqlTask(this, CheckIfExistsSql) { TaskName = $"Check if view {ViewName} exists", TaskHash = this.TaskHash }.ExecuteScalarAsBool();
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        /* Public properties */
        public string ViewName { get; set; }
        public string Definition { get; set; }
        public string Sql => $@"{CreateOrAlterSql} VIEW {ViewName}
AS
{Definition}
";

        public CRUDViewTask() {

        }
        public CRUDViewTask(string viewName, string definition) : this() {
            this.ViewName = viewName;
            this.Definition = definition;
        }

        public static void CreateOrAlter(string viewName, string definition) => new CRUDViewTask(viewName, definition).Execute();
        public static void CreateOrAlter(IConnectionManager connectionManager, string viewName, string definition) => new CRUDViewTask(viewName, definition) { ConnectionManager = connectionManager }.Execute();

        string CheckIfExistsSql => $@"IF EXISTS (SELECT * FROM sys.objects WHERE type = 'V' AND object_id = object_id('{ViewName}')) SELECT 1; 
ELSE SELECT 0;";
        bool IsExisting { get; set; }
        string CreateOrAlterSql => IsExisting ? "ALTER" : "CREATE";

    }
}
