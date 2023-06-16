﻿using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.Logging;
using NLog;
using NLog.Config;

namespace ALE.ETLBox.ControlFlow
{
    /// <summary>
    /// Contains static information which affects all ETLBox tasks.
    /// Here you can set default connections string, disbale the logging for all processes or set the current stage used in your logging configuration.
    /// </summary>
    [PublicAPI]
    public static class ControlFlow
    {
        private static IConnectionManager s_defaultDbConnection;

        /// <summary>
        /// You can store your general database connection string here. This connection will then used by all Tasks where no DB connection is excplicitly set.
        /// </summary>
        public static IConnectionManager DefaultDbConnection
        {
            get
            {
                return s_defaultDbConnection
                    ?? throw new ETLBoxException(
                        "No connection manager found! The component or task you are "
                            + "using expected a  connection manager to connect to the database."
                            + "Either pass a connection manager or set a default connection manager within the "
                            + "ControlFlow.DefaultDbConnection property!"
                    );
            }
            set { s_defaultDbConnection = value; }
        }

        /// <summary>
        /// If set to true, nothing will be logged by any control flow task or data flow component.
        /// When switched back to false, all tasks and components will continue to log.
        /// </summary>
        public static bool DisableAllLogging { get; set; }

        /// <summary>
        /// For logging purposes only. If the stage is set, you can access the stage value in the logging configuration.
        /// </summary>
        public static string Stage { get; set; }

        /// <summary>
        /// If you used the logging task StartLoadProces (and created the corresponding load process table before)
        /// then this Property will hold the current load process information.
        /// </summary>
        public static LoadProcess CurrentLoadProcess { get; internal set; }

        public const string DefaultLoadProcessTableName = "etlbox_loadprocess";

        /// <summary>
        /// TableName of the current load process logging table
        /// </summary>
        public static string LoadProcessTable { get; set; } = DefaultLoadProcessTableName;

        public const string DefaultLogTableName = "etlbox_log";

        /// <summary>
        /// TableName of the current log process logging table
        /// </summary>
        public static string LogTable { get; set; } = DefaultLogTableName;

        public static void AddLoggingDatabaseToConfig(IConnectionManager connection) =>
            AddLoggingDatabaseToConfig(connection, LogLevel.Info);

        /// <summary>
        /// You can also set the logging database in the nlog.config file.
        /// If you want to programmatically change the logging database,  use this method.
        /// </summary>
        /// <param name="connection">The new logging database connection manager</param>
        /// <param name="minLogLevel">Logging level</param>
        /// <param name="logTableName">Table to hold logs</param>
        public static void AddLoggingDatabaseToConfig(
            IConnectionManager connection,
            LogLevel minLogLevel,
            string logTableName = DefaultLogTableName
        )
        {
            try
            {
                if (
                    LogTable != null
                    && LogTable != DefaultLoadProcessTableName
                    && logTableName == DefaultLoadProcessTableName
                )
                    logTableName = LogTable;
                var newTarget = new CreateDatabaseTarget(
                    connection,
                    logTableName
                ).GetNLogDatabaseTarget();
                LoggingConfiguration config = new LoggingConfiguration();
                config.AddRule(minLogLevel, LogLevel.Fatal, newTarget);
                LogManager.Setup().LoadConfiguration(config);
            }
            catch
            {
                // ignored
            }
        }

        private static bool s_isLayoutRendererRegistered;

        public static Logger GetLogger()
        {
            if (s_isLayoutRendererRegistered)
            {
                return LogManager.GetLogger("ETL");
            }

            LogManager
                .Setup()
                .SetupExtensions(builder =>
                {
                    builder.RegisterLayoutRenderer<ETLLogLayoutRenderer>("etllog");
                });
            s_isLayoutRendererRegistered = true;
            return LogManager.GetLogger("ETL");
        }

        /// <summary>
        /// Set all settings back to default (which is null or false)
        /// </summary>
        public static void ClearSettings()
        {
            DefaultDbConnection = null;
            CurrentLoadProcess = null;
            DisableAllLogging = false;
            LoadProcessTable = DefaultLoadProcessTableName;
            LogTable = DefaultLogTableName;
            Stage = null;
        }
    }
}
