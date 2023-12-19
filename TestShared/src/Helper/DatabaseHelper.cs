using ALE.ETLBox.ControlFlow;
using ETLBox.Primitives;

namespace TestShared.Helper
{
    public static class DatabaseHelper
    {
        private static void DropAndCreate<TConnectionManager>(
            TConnectionManager connManagerMaster,
            string dbName
        )
            where TConnectionManager : IConnectionManager
        {
            DropDatabase(connManagerMaster, dbName);
            CreateDatabase(connManagerMaster, dbName);
        }

        private static void CreateDatabase<TConnectionManager>(
            TConnectionManager connManagerMaster,
            string dbName
        )
            where TConnectionManager : IConnectionManager
        {
            new CreateDatabaseTask(dbName)
            {
                DisableLogging = true,
                ConnectionManager = connManagerMaster
            }.Execute();
        }

        private static void DropDatabase<TConnectionManager>(
            TConnectionManager connManagerMaster,
            string dbName
        )
            where TConnectionManager : IConnectionManager
        {
            new DropDatabaseTask(dbName)
            {
                DisableLogging = true,
                ConnectionManager = connManagerMaster
            }.DropIfExists();
        }

        private static void ActOnDatabase<TConnectionManager, TConnectionString>(
            Config.ConnectionDetails<TConnectionString, TConnectionManager> connectionDetails,
            string section,
            string dbNameSuffix,
            Action<TConnectionManager, string> databaseAction
        )
            where TConnectionManager : IConnectionManager, new()
            where TConnectionString : IDbConnectionString, new()
        {
            var connManagerMaster = new TConnectionManager
            {
                ConnectionString = connectionDetails
                    .ConnectionString(section)
                    .CloneWithMasterDbName()
            };
            var dbName =
                connectionDetails.ConnectionString(section).DbName + (dbNameSuffix ?? string.Empty);
            databaseAction(connManagerMaster, dbName);
        }

        public static void DropDatabase<TConnectionManager, TConnectionString>(
            Config.ConnectionDetails<TConnectionString, TConnectionManager> connectionDetails,
            string section,
            string dbNameSuffix = null
        )
            where TConnectionManager : IConnectionManager, new()
            where TConnectionString : IDbConnectionString, new() =>
            ActOnDatabase(connectionDetails, section, dbNameSuffix, DropDatabase);

        public static void CreateDatabase<TConnectionManager, TConnectionString>(
            Config.ConnectionDetails<TConnectionString, TConnectionManager> connectionDetails,
            string section,
            string dbNameSuffix = null
        )
            where TConnectionManager : IConnectionManager, new()
            where TConnectionString : IDbConnectionString, new() =>
            ActOnDatabase(connectionDetails, section, dbNameSuffix, CreateDatabase);

        public static void RecreateDatabase<TConnectionManager, TConnectionString>(
            Config.ConnectionDetails<TConnectionString, TConnectionManager> connectionDetails,
            string section,
            string dbNameSuffix = null
        )
            where TConnectionManager : IConnectionManager, new()
            where TConnectionString : IDbConnectionString, new() =>
            ActOnDatabase(connectionDetails, section, dbNameSuffix, DropAndCreate);
    }
}
