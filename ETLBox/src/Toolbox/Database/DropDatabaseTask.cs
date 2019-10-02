using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Tries to drop a database if the database exists.
    /// </summary>
    /// <example>
    /// <code>
    /// DropDatabaseTask.Delete("DemoDB");
    /// </code>
    /// </example>
    public class DropDatabaseTask : DropTask<IfDatabaseExistsTask>, ITask
    {
        internal override string GetSql()
        {

            if (ConnectionType == ConnectionManagerType.SqlServer)
            {
                return
$@"
USE [master]
ALTER DATABASE [{ObjectName}]
SET SINGLE_USER WITH ROLLBACK IMMEDIATE
ALTER DATABASE [{ObjectName}]
SET MULTI_USER
DROP DATABASE [{ObjectName}]  
";
            }
            else if (ConnectionType == ConnectionManagerType.SQLite)
            {
                throw new ETLBoxNotSupportedException("This task is not supported with SQLite!");
            }
            else 
            {
                return $@"DROP DATABASE {QB}{ObjectName}{QE}";
            }
        }

        /* Some constructors */
        public DropDatabaseTask()
        {
        }

        public DropDatabaseTask(string databaseName) : this()
        {
            ObjectName = databaseName;
        }


        /* Static methods for convenience */
        public static void Drop(string databaseName)
            => new DropDatabaseTask(databaseName).Drop();
        public static void Drop(IConnectionManager connectionManager, string databaseName)
            => new DropDatabaseTask(databaseName) { ConnectionManager = connectionManager }.Drop();
        public static void DropIfExists(string databaseName)
            => new DropDatabaseTask(databaseName).DropIfExists();
        public static void DropIfExists(IConnectionManager connectionManager, string databaseName)
            => new DropDatabaseTask(databaseName) { ConnectionManager = connectionManager }.DropIfExists();
    }
}
