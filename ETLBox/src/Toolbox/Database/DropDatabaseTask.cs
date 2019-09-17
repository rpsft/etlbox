using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow {
    /// <summary>
    /// Tries to drop a database if the database exists.
    /// </summary>
    /// <example>
    /// <code>
    /// DropDatabaseTask.Delete("DemoDB");
    /// </code>
    /// </example>
    public class DropDatabaseTask : GenericTask, ITask
    {
        /* ITask Interface */
        public override string TaskType { get; set; } = "DROPDB";
        public override string TaskName => $"Drop DB {DatabaseName}";
        public override void Execute()
        {
            if (ConnectionType == ConnectionManagerType.SQLite)
                throw new ETLBoxNotSupportedException("This task is not supported with SQLite!");
            new SqlTask(this, Sql).ExecuteNonQuery();
        }

        public void Drop() => Execute();

        /* Public properties */
        public string DatabaseName { get; set; }
        public string Sql
        {
            get
            {
                if (ConnectionType == ConnectionManagerType.SqlServer)
                {
                    return
    $@"
IF (db_id('{DatabaseName}') IS NOT NULL)
BEGIN
    USE [master]
    ALTER DATABASE [{DatabaseName}]
    SET SINGLE_USER WITH ROLLBACK IMMEDIATE
    ALTER DATABASE [{DatabaseName}]
    SET MULTI_USER
    DROP DATABASE [{DatabaseName}]  
END
";
                }
                else if (ConnectionType == ConnectionManagerType.MySql)
                {
                    return $@"DROP DATABASE IF EXISTS {DatabaseName}";
                }
                else
                {
                    return $@"DROP DATABASE {DatabaseName}";
                }
            }
        }

        /* Some constructors */
        public DropDatabaseTask() {
        }

        public DropDatabaseTask(string databaseName) : this()
        {
            DatabaseName = databaseName;
        }


        /* Static methods for convenience */
        public static void Drop(string databaseName) => new DropDatabaseTask(databaseName).Execute();
        public static void Drop(IConnectionManager connectionManager, string databaseName) => new DropDatabaseTask(databaseName) { ConnectionManager = connectionManager }.Execute();

        /* Implementation & stuff */
    }


}
