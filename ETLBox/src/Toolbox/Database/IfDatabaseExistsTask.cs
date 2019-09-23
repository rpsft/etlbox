using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Check if a Database exists. 
    /// </summary>
    public class IfDatabaseExistsTask : IfExistsTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "IFDBEXISTS_DB";

        internal override string GetSql()
        {
            if (this.ConnectionType == ConnectionManagerType.SqlServer)
            {
                return $@"SELECT COUNT(*) FROM sys.databases WHERE [NAME] = '{ObjectName}'";
            }
            else if (this.ConnectionType == ConnectionManagerType.MySql)
            {
                return $@"SELECT COUNT(*)  FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{ObjectName}'";
            }
            else if (this.ConnectionType == ConnectionManagerType.Postgres)
            {
                return $@"SELECT COUNT(*) FROM pg_database WHERE datname = '{ObjectName}'";
            }
            else
            {
                return string.Empty;
            }
        }
        /* Some constructors */
        public IfDatabaseExistsTask() : base()
        {
        }

        public IfDatabaseExistsTask(string databaseName) : this()
        {
            ObjectName = databaseName;
        }


        /* Static methods for convenience */
        public static bool IsExisting(string databaseName) => new IfDatabaseExistsTask(databaseName).Exists();
        public static bool IsExisting(IConnectionManager connectionManager, string databaseName)
            => new IfDatabaseExistsTask(databaseName) { ConnectionManager = connectionManager }.Exists();
    }
}