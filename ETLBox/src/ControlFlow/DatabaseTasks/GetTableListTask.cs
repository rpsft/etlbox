using ETLBox.Connection;
using ETLBox.Helper;
using System;
using System.Collections.Generic;

namespace ETLBox.ControlFlow.Tasks
{
    /// <summary>
    /// Returns a list of all tables in the currently connected database.     
    /// Make sure to connect with the correct permissions!
    /// </summary>
    /// <example>
    /// <code>    
    /// List&lt;ETLBox.Helper.ObjectNameDescriptor&gt; alltables = GetTableListTask.List();
    /// foreach (var on in alltables) {
    ///     Console.WriteLine("Schema:" + on.UnquotatedSchemaName);
    ///     Console.WriteLine("TableName:" + on.UnquotatedObjectName);
    ///     Console.WriteLine("Full qualified name:" + on.QuotatedFullName);
    /// }
    /// </code>
    /// </example>
    public sealed class GetTableListTask : GetListTask
    {
        /// <inheritdoc/>
        public override string TaskName { get; set; } = $"Get a list of all tables in the current database.";

        public GetTableListTask()
        {

        }

        internal override string GetSql()
        {
            if (this.ConnectionType == ConnectionManagerType.SQLite)
                return $@"SELECT tbl_name FROM sqlite_master WHERE type = 'table'";
            else if (this.ConnectionType == ConnectionManagerType.SqlServer)
                return $@"
SELECT '['+sc.name+'].['+tbl.name+']' FROM sys.tables tbl
INNER JOIN sys.schemas sc
  ON tbl.schema_id = sc.schema_id
";
            else if (this.ConnectionType == ConnectionManagerType.MySql)
                return $@"
SELECT TABLE_NAME
FROM information_schema.tables
WHERE table_schema = DATABASE()
AND TABLE_TYPE = 'BASE TABLE'
";
            else if (this.ConnectionType == ConnectionManagerType.Postgres)
                return $@"
SELECT '""'||table_schema||'"".""'||table_name||'""',*
FROM information_schema.tables
WHERE table_catalog = CURRENT_DATABASE()
AND table_type = 'BASE TABLE'
AND table_schema NOT IN('pg_catalog', 'information_schema')
";
            else if (this.ConnectionType == ConnectionManagerType.Oracle)
                return $@"SELECT TABLE_NAME FROM USER_TABLES";
            else if (this.ConnectionType == ConnectionManagerType.Db2)
                //return $@"SELECT NAME FROM SYSIBM.SYSTABLES WHERE CREATOR = CURRENT USER AND TYPE = 'T'";
                return $@"SELECT TABNAME FROM SYSCAT.TABLES WHERE TABSCHEMA = CURRENT USER AND TYPE = 'T'";
            else
                throw new NotSupportedException($"The database type {this.ConnectionType} is not supported for this task!");
        }

        internal override void CleanUpRetrievedList()
        {

        }

        /// <summary>
        /// Runs sql code to determine all user database names.
        /// </summary>
        /// <returns>A list of all user database names</returns>
        public static List<ObjectNameDescriptor> ListAll()
            => new GetTableListTask().RetrieveAll().ObjectNames;

        /// <summary>
        /// Runs sql code to determine all user database names.
        /// </summary>
        /// <param name="connectionManager">The connection manager of the server you want to connect</param>
        /// <returns>A list of all user database names</returns>
        public static List<ObjectNameDescriptor> ListAll(IConnectionManager connectionManager)
            => new GetTableListTask() { ConnectionManager = connectionManager }.RetrieveAll().ObjectNames;

    }
}
