using ETLBox.Connection;
using ETLBox.Exceptions;
using ETLBox.Logging;
using NLog;

namespace ETLBox.Logging
{
    /// <summary>
    /// Contains static information which affects all ETLBox tasks and general logging behavior for all components.
    /// Here you can set default connections string, disbale the logging for all processes or set the current stage used in your logging configuration.
    /// </summary>
    public static class Logging
    {
        /// <summary>
        /// If set to true, nothing will be logged by any control flow task or data flow component.
        /// When switched back to false, all tasks and components will continue to log.
        /// </summary>
        public static bool DisableAllLogging { get; set; }

        /// <summary>
        /// For logging purposes only. If the stage is set, you can access the stage value in the logging configuration.
        /// </summary>
        public static string STAGE { get; set; }

        /// <summary>
        /// If you used the logging task StartLoadProces (and created the corresponding load process table before)
        /// then this Property will hold the current load process information.
        /// </summary>
        public static LoadProcess CurrentLoadProcess { get; internal set; }

        public const string DEFAULTLOADPROCESSTABLENAME = "etlbox_loadprocess";
        /// <summary>
        /// TableName of the current load process logging table
        /// </summary>
        public static string LoadProcessTable { get; set; } = DEFAULTLOADPROCESSTABLENAME;

        /// <summary>
        /// The default log table name
        /// </summary>
        public const string DEFAULTLOGTABLENAME = "etlbox_log";

        /// <summary>
        /// TableName of the current log process logging table
        /// </summary>
        public static string LogTable { get; set; } = DEFAULTLOGTABLENAME;

        /// <summary>
        /// You can also set the logging database in the nlog.config file.
        /// If you want to programmatically change the logging database,  use this method.
        /// </summary>
        /// <param name="connection">The new logging database connection manager</param>

        public static void AddLoggingDatabaseToConfig(IConnectionManager connection) => AddLoggingDatabaseToConfig(connection, LogLevel.Info);

        /// <summary>
        /// You can also set the logging database in the nlog.config file.
        /// If you want to programmatically change the logging database,  use this method.
        /// </summary>
        /// <param name="connection">The new logging database connection manager</param>
        /// <param name="minLogLevel">The minimum log level at which logs are written into the database</param>
        /// <param name="logTableName">The name of the log table where the logs are written into</param>
        public static void AddLoggingDatabaseToConfig(IConnectionManager connection, LogLevel minLogLevel, string logTableName = DEFAULTLOGTABLENAME)
        {

            try
            {
                if (LogTable != null && LogTable != DEFAULTLOADPROCESSTABLENAME && logTableName == DEFAULTLOADPROCESSTABLENAME)
                    logTableName = LogTable;
                var newTarget = new CreateDatabaseTarget(connection, logTableName).GetNLogDatabaseTarget();
                NLog.Config.SimpleConfigurator.ConfigureForTargetLogging(newTarget, minLogLevel);
            }
            catch
            {
                ;
            }
        }

        static bool IsLayoutRendererRegisterd = false;

        /// <summary>
        /// Returns the current NLog Logger instance used for the default ETL logging
        /// </summary>
        /// <returns></returns>
        public static NLog.Logger GetLogger()
        {
            if (!IsLayoutRendererRegisterd)
            {
                NLog.Config.ConfigurationItemFactory.Default.LayoutRenderers.RegisterDefinition("etllog", typeof(ETLLogLayoutRenderer));
                IsLayoutRendererRegisterd = true;
            }
            return NLog.LogManager.GetLogger("ETL");
        }

        /// <summary>
        /// Set all settings back to default (which is null or false)
        /// </summary>
        public static void ClearSettings()
        {
            CurrentLoadProcess = null;
            DisableAllLogging = false;
            LoadProcessTable = DEFAULTLOADPROCESSTABLENAME;
            LogTable = DEFAULTLOGTABLENAME;
            STAGE = null;
        }
    }



}
