using System;
using ETLBox.Primitives;
using JetBrains.Annotations;
using NLog.Layouts;
using NLog.Targets;

namespace ALE.ETLBox.Common.Logging
{
    [PublicAPI]
    internal class CreateDatabaseTarget
    {
        public ObjectNameDescriptor TN =>
            new(LogTableName, ConnectionManager.QB, ConnectionManager.QE);
        public string QB => ConnectionManager.QB;
        public string QE => ConnectionManager.QE;
        public string CommandText =>
            $@"
INSERT INTO {TN.QuotedFullName}
    ( {QB}log_date{QE}, {QB}level{QE}, {QB}stage{QE}, {QB}message{QE}, {QB}task_type{QE}, {QB}task_action{QE}, {QB}task_hash{QE}, {QB}source{QE}, {QB}load_process_id{QE})
SELECT {LogDate}
    , @Level
    , CAST(@Stage as {Varchar}(20))
    , CAST(@Message as {Varchar}(4000))
    , CAST(@Type as {Varchar}(40))
    , @Action
    , @Hash
    , CAST(@Logger as {Varchar}(20))
    , CASE WHEN @LoadProcessKey IS NULL OR @LoadProcessKey = '' OR @LoadProcessKey = '0'
           THEN NULL
           ELSE CAST(@LoadProcessKey AS {Int}) END 
";
        public string LogDate
        {
            get
            {
                return ConnectionManager.ConnectionManagerType switch
                {
                    ConnectionManagerType.SqlServer
                    or ConnectionManagerType.MySql
                        => "CAST(@LogDate AS DATETIME)",
                    ConnectionManagerType.Postgres => "CAST(@LogDate AS TIMESTAMP)",
                    _ => "@LogDate"
                };
            }
        }
        public string Varchar =>
            ConnectionManager.ConnectionManagerType == ConnectionManagerType.MySql
                ? "CHAR"
                : "VARCHAR";
        public string Int =>
            ConnectionManager.ConnectionManagerType == ConnectionManagerType.MySql
                ? "UNSIGNED"
                : "INT";

        public IConnectionManager ConnectionManager { get; set; }
        public string LogTableName { get; set; }

        public CreateDatabaseTarget(IConnectionManager connectionManager, string logTableName)
        {
            ConnectionManager = connectionManager;
            LogTableName = logTableName;
        }

        public DatabaseTarget GetNLogDatabaseTarget()
        {
            DatabaseTarget dbTarget = new DatabaseTarget();
            AddParameter(dbTarget, "LogDate", @"${date:format=yyyy-MM-dd HH\:mm\:ss.fff}");
            AddParameter(dbTarget, "Level", @"${level}");
            AddParameter(dbTarget, "Stage", @"${etllog:LogType=Stage}");
            AddParameter(dbTarget, "Message", @"${etllog}");
            AddParameter(dbTarget, "Type", @"${etllog:LogType=Type}");
            AddParameter(dbTarget, "Action", @"${etllog:LogType=Action}");
            AddParameter(dbTarget, "Hash", @"${etllog:LogType=Hash}");
            AddParameter(dbTarget, "LoadProcessKey", @"${etllog:LogType=LoadProcessKey}");
            AddParameter(dbTarget, "Logger", @"${logger}");

            dbTarget.CommandText = new SimpleLayout(CommandText);
            dbTarget.DBProvider = ConnectionManager.ConnectionManagerType switch
            {
                ConnectionManagerType.Postgres => "Npgsql.NpgsqlConnection, Npgsql",
                ConnectionManagerType.MySql => "MySql.Data.MySqlClient.MySqlConnection, MySql.Data",
                ConnectionManagerType.SQLite
                    => "Microsoft.Data.Sqlite.SqliteConnection, Microsoft.Data.Sqlite",
                ConnectionManagerType.SqlServer
                    => "Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient",
                _ => throw new NotSupportedException("Only SQL fatabases are supported for logs")
            };
            dbTarget.ConnectionString = ConnectionManager.ConnectionString.Value;
            return dbTarget;
        }

        private static void AddParameter(
            DatabaseTarget dbTarget,
            string parameterName,
            string layout
        )
        {
            var par = new DatabaseParameterInfo(parameterName, new SimpleLayout(layout));
            dbTarget.Parameters.Add(par);
        }
    }
}
