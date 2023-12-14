using JetBrains.Annotations;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace EtlBox.Logging.Configuration
{
    /// <summary>
    /// Contains static information which affects all ETLBox tasks.
    /// Here you can set default connections string, disbale the logging for all processes or set the current stage used in your logging configuration.
    /// </summary>
    [PublicAPI]
    public static class LoggingFactory
    {
        private static NullLoggerFactory s_defaultLoggingFactory;

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


        /// <summary>
        /// You can also set the logging database in the nlog.config file.
        /// If you want to programmatically change the logging database,  use this method.
        /// </summary>
        /// <param name="connection">The new logging database connection manager</param>
        /// <param name="minLogLevel">Logging level</param>
        /// <param name="logTableName">Table to hold logs</param>
        public static void AddLoggingConfiguration(

        )
        {
            s_defaultLoggingFactory = NullLoggerFactory.Instance;

            s_defaultLoggingFactory = new LoggerFactory(builder)
        }

        private static bool s_isLayoutRendererRegistered;

        public static ILogger GetLogger()
        {
            return s_defaultLoggingFactory.GetLo
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
