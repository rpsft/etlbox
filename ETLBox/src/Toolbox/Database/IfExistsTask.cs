using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Drops a table if the table exists.
    /// </summary>
    public class IfExistsTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "IFEXISTS";
        public override string TaskName => $"Check if {ObjectName} exists";
        public override void Execute()
        {
            if (Sql != string.Empty)
                DoesExist = new SqlTask(this, Sql).ExecuteScalarAsBool();
        }

        /* Public properties */
        public string ObjectName { get; set; }
        public bool DoesExist { get; private set; }

        public string Sql
        {
            get
            {
                if (this.ConnectionType == ConnectionManagerType.SQLLite)
                {
                    return $@"SELECT 1 FROM sqlite_master WHERE name='{ObjectName}';";
                }
                else if (this.ConnectionType == ConnectionManagerType.SqlServer)
                {
                    return
        $@"IF ( OBJECT_ID('{ObjectName}', 'U') IS NOT NULL OR OBJECT_ID('{ObjectName}', 'V') IS NOT NULL)
  SELECT 1";
                }
                else
                {
                    return string.Empty;
                }
            }
        }

        public void Drop() => Execute();

        /* Some constructors */
        public IfExistsTask()
        {
        }

        public IfExistsTask(string objectName) : this()
        {
            ObjectName = objectName;
        }

        public bool Exists()
        {
            Execute();
            return DoesExist;
        }

        /* Static methods for convenience */
        public static bool IsExisting(string objectName) => new IfExistsTask(objectName).Exists();
        public static bool IsExisting(IConnectionManager connectionManager, string objectName)
            => new IfExistsTask(objectName) { ConnectionManager = connectionManager }.Exists();

        public static void ThrowExceptionIfNotExists(IConnectionManager connectionManager, string objectName)
        {
            bool tableExists = new IfExistsTask(objectName)
            {
                ConnectionManager = connectionManager,
                DisableLogging = true
            }.Exists();
            if (!tableExists)
                throw new ETLBoxException($"An object {objectName} does not exists in the database!");

        }
    }
}