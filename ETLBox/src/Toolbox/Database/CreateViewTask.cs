using ETLBox.Connection;
using ETLBox.Helper;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Creates or updates a view.
    /// </summary>
    public class CreateViewTask : GenericTask, ITask
    {
        public override string TaskName => $"Create or alter view {ViewName ?? string.Empty}";
        public void Execute()
        {
            IsExisting = new IfTableOrViewExistsTask(ViewName) { ConnectionManager = this.ConnectionManager, DisableLogging = true }.Exists();
            if (
                (ConnectionType == ConnectionManagerType.SQLite || ConnectionType == ConnectionManagerType.Access)
                && IsExisting
                )
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
        string CreateOrAlterSql {
            get {
                if (!IsExisting) {
                    return "CREATE";
                }
                else {
                    if (ConnectionType == ConnectionManagerType.SQLite || ConnectionType == ConnectionManagerType.Access)
                        return "CREATE";
                    else if (ConnectionType == ConnectionManagerType.Postgres || ConnectionType == ConnectionManagerType.Oracle)
                        return "CREATE OR REPLACE";
                    else
                        return "ALTER";
                }
            }
        }


    }
}
