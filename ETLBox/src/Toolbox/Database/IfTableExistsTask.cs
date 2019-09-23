using ALE.ETLBox.ConnectionManager;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Drops a table if the table exists.
    /// </summary>
    public class IfTableExistsTask : IfExistsTask, ITask
    {
        /* ITask Interface */
        //        public override string TaskType { get; set; } = "IFEXISTS";
        //        public override string TaskName => $"Check if {ObjectName} exists";
        //        public override void Execute()
        //        {
        //            if (Sql != string.Empty)
        //                DoesExist = new SqlTask(this, Sql).ExecuteScalarAsBool();
        //        }

        //        /* Public properties */
        //        public string ObjectName { get; set; }
        //        public bool DoesExist { get; private set; }

        //        public string Sql
        //        {
        //            get
        //            {
        //                if (this.ConnectionType == ConnectionManagerType.SQLite)
        //                {
        //                    return $@"SELECT 1 FROM sqlite_master WHERE name='{ObjectName}';";
        //                }
        //                else if (this.ConnectionType == ConnectionManagerType.SqlServer)
        //                {
        //                    return
        //        $@"
        //IF EXISTS (SELECT *  FROM sys.indexes  WHERE name='{ObjectName}' )
        //    SELECT 1
        //IF ( OBJECT_ID('{ObjectName}') IS NOT NULL)
        //    SELECT 1";
        //                }
        //                else if (this.ConnectionType == ConnectionManagerType.MySql)
        //                {
        //                    return $@"SELECT EXISTS(
        //    SELECT table_name
        //    FROM information_schema.tables
        //    WHERE table_schema = DATABASE()
        //    AND ( table_name = '{ObjectName}' OR CONCAT(table_catalog, '.', table_name) = '{ObjectName}')
        //) AS 'DoesExist'";
        //                }
        //                else if (this.ConnectionType == ConnectionManagerType.Postgres)
        //                {
        //                    return $@"SELECT EXISTS(
        //    SELECT table_name
        //    FROM information_schema.tables
        //    WHERE table_catalog = CURRENT_DATABASE()
        //    AND ( table_name = '{ObjectName}' OR CONCAT(table_schema, '.', table_name) = '{ObjectName}')
        //)";
        //                }
        //                else
        //                {
        //                    return string.Empty;
        //                }
        //            }
        //        }

        //        /* Some constructors */
        //        public IfTableExistsTask()
        //        {
        //        }

        //        public IfTableExistsTask(string objectName) : this()
        //        {
        //            ObjectName = objectName;
        //        }

        //        public bool Exists()
        //        {
        //            Execute();
        //            return DoesExist;
        //        }

        //        /* Static methods for convenience */
        //        public static bool IsExisting(string objectName) => new IfTableExistsTask(objectName).Exists();
        //        public static bool IsExisting(IConnectionManager connectionManager, string objectName)
        //            => new IfTableExistsTask(objectName) { ConnectionManager = connectionManager }.Exists();

        /* ITask Interface */
        public override string TaskType { get; set; } = "IFDBEXISTS_TABLE";

        internal override string GetSql()
        {
            if (this.ConnectionType == ConnectionManagerType.SQLite)
            {
                return $@"SELECT 1 FROM sqlite_master WHERE name='{ObjectName}';";
            }
            else if (this.ConnectionType == ConnectionManagerType.SqlServer)
            {
                return
    $@"
        IF EXISTS (SELECT *  FROM sys.indexes  WHERE name='{ObjectName}' )
            SELECT 1
        IF ( OBJECT_ID('{ObjectName}') IS NOT NULL)
            SELECT 1";
            }
            else if (this.ConnectionType == ConnectionManagerType.MySql)
            {
                return $@"SELECT EXISTS(
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND ( table_name = '{ObjectName}' OR CONCAT(table_catalog, '.', table_name) = '{ObjectName}')
        ) AS 'DoesExist'";
            }
            else if (this.ConnectionType == ConnectionManagerType.Postgres)
            {
                return $@"SELECT EXISTS(
            SELECT table_name
            FROM information_schema.tables
            WHERE table_catalog = CURRENT_DATABASE()
            AND ( table_name = '{ObjectName}' OR CONCAT(table_schema, '.', table_name) = '{ObjectName}')
        )";
            }
            else
            {
                return string.Empty;
            }
        }

        /* Some constructors */
        public IfTableExistsTask() : base()
        {
        }

        public IfTableExistsTask(string tableName) : this()
        {
            ObjectName = tableName;
        }


        /* Static methods for convenience */
        public static bool IsExisting(string objectName) => new IfTableExistsTask(objectName).Exists();
        public static bool IsExisting(IConnectionManager connectionManager, string objectName)
            => new IfTableExistsTask(objectName) { ConnectionManager = connectionManager }.Exists();


        public static void ThrowExceptionIfNotExists(IConnectionManager connectionManager, string tableName)
        {
            bool tableExists = new IfTableExistsTask(tableName)
            {
                ConnectionManager = connectionManager,
                DisableLogging = true
            }.Exists();
            if (!tableExists)
                throw new ETLBoxException($"An table {tableName} does not exists in the database!");
        }
    }
}