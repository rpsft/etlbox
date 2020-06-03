using ETLBox.Connection;
using ETLBox.Helper;
using NLog.Targets;

namespace ETLBox.Logging
{

    public class CreateDatabaseTarget
    {
        public ObjectNameDescriptor TN => new ObjectNameDescriptor(LogTableName, ConnectionManager.QB, ConnectionManager.QE);
        public string QB => ConnectionManager.QB;
        public string QE => ConnectionManager.QE;
        public string CommandText => $@"
INSERT INTO {TN.QuotatedFullName}
    ( {QB}log_date{QE}, {QB}level{QE}, {QB}stage{QE}, {QB}message{QE}, {QB}task_type{QE}, {QB}task_action{QE}, {QB}task_hash{QE}, {QB}source{QE}, {QB}load_process_id{QE})
SELECT {LogDate}
    , @Level
    , CAST(@Stage as {VARCHAR}(20))
    , CAST(@Message as {VARCHAR}(4000))
    , CAST(@Type as {VARCHAR}(40))
    , @Action
    , @Hash
    , CAST(@Logger as {VARCHAR}(20))
    , CASE WHEN @LoadProcessKey IS NULL OR @LoadProcessKey = '' OR @LoadProcessKey = '0'
           THEN NULL
           ELSE CAST(@LoadProcessKey AS {INT}) END 
";
        public string LogDate
        {
            get
            {
                if (ConnectionManager.ConnectionManagerType == ConnectionManagerType.SqlServer || ConnectionManager.ConnectionManagerType == ConnectionManagerType.MySql)
                    return "CAST(@LogDate AS DATETIME)";
                else if (ConnectionManager.ConnectionManagerType == ConnectionManagerType.Postgres)
                    return "CAST(@LogDate AS TIMESTAMP)";
                else
                    return "@LogDate";
            }
        }
        public string VARCHAR => this.ConnectionManager.ConnectionManagerType == ConnectionManagerType.MySql ? "CHAR" : "VARCHAR";
        public string INT => this.ConnectionManager.ConnectionManagerType == ConnectionManagerType.MySql ? "UNSIGNED" : "INT";

        public IConnectionManager ConnectionManager { get; set; }
        public string LogTableName { get; set; }

        public CreateDatabaseTarget(IConnectionManager connectionManager, string logTableName)
        {
            this.ConnectionManager = connectionManager;
            this.LogTableName = logTableName;
        }

        public DatabaseTarget GetNLogDatabaseTarget()
        {
            DatabaseTarget dbTarget = new DatabaseTarget();
            //foreach (var par in dbTarget.Parameters)
            AddParameter(dbTarget, "LogDate", @"${date:format=yyyy-MM-dd HH\:mm\:ss.fff}");
            AddParameter(dbTarget, "Level", @"${level}");
            AddParameter(dbTarget, "Stage", @"${etllog:LogType=Stage}");
            AddParameter(dbTarget, "Message", @"${etllog}");
            AddParameter(dbTarget, "Type", @"${etllog:LogType=Type}");
            AddParameter(dbTarget, "Action", @"${etllog:LogType=Action}");
            AddParameter(dbTarget, "Hash", @"${etllog:LogType=Hash}");
            AddParameter(dbTarget, "LoadProcessKey", @"${etllog:LogType=LoadProcessKey}");
            AddParameter(dbTarget, "Logger", @"${logger}");

            dbTarget.CommandText = new NLog.Layouts.SimpleLayout(CommandText);
            if (ConnectionManager.ConnectionManagerType == ConnectionManagerType.Postgres)
                dbTarget.DBProvider = "Npgsql.NpgsqlConnection, Npgsql";
            else if (ConnectionManager.ConnectionManagerType == ConnectionManagerType.MySql)
                dbTarget.DBProvider = "MySql.Data.MySqlClient.MySqlConnection, MySql.Data";
            else if (ConnectionManager.ConnectionManagerType == ConnectionManagerType.SQLite)
                dbTarget.DBProvider = "System.Data.SQLite.SQLiteConnection, System.Data.SQLite";
            else
                dbTarget.DBProvider = "Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient";
            dbTarget.ConnectionString = ConnectionManager.ConnectionString.Value;
            return dbTarget;
        }

        private void AddParameter(DatabaseTarget dbTarget, string parameterName, string layout)
        {
            var par = new DatabaseParameterInfo(parameterName, new NLog.Layouts.SimpleLayout(layout));
            dbTarget.Parameters.Add(par);
        }
    }
}
