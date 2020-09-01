using ETLBox.Connection;
using ETLBox.Helper;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Creates or alters a view.
    /// </summary>
    /// <example>
    /// <code>
    /// CreateViewTask.CreateOrAlter("viewname","SELECT value FROM table");
    /// </code>
    /// </example>
    public class CreateViewTask : ControlFlowTask
    {
        /// <inheritdoc/>
        public override string TaskName => $"Create or alter view {ViewName ?? string.Empty}";

        /// <summary>
        /// Executes the creation/altering of the view.
        /// </summary>
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

        /// <summary>
        /// The name of the view
        /// </summary>
        public string ViewName { get; set; }

        /// <summary>
        /// The formatted name of the view
        /// </summary>
        public ObjectNameDescriptor VN => new ObjectNameDescriptor(ViewName, QB, QE);

        /// <summary>
        /// The view definition.
        /// </summary>
        public string Definition { get; set; }

        /// <summary>
        /// The sql that is generated to create the view
        /// </summary>
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

        /// <summary>
        /// Creates or alter a view.
        /// </summary>
        /// <param name="viewName">The name of the view</param>
        /// <param name="definition">The view definition</param>
        public static void CreateOrAlter(string viewName, string definition) => new CreateViewTask(viewName, definition).Execute();

        /// <summary>
        /// Creates or alter a view.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the database you want to connect</param>
        /// <param name="viewName">The name of the view</param>
        /// <param name="definition">The view definition</param>
        public static void CreateOrAlter(IConnectionManager connectionManager, string viewName, string definition) => new CreateViewTask(viewName, definition) { ConnectionManager = connectionManager }.Execute();

        string CreateViewName => ConnectionType == ConnectionManagerType.Access ? VN.UnquotatedFullName : VN.QuotatedFullName;
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
