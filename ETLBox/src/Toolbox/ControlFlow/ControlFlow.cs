using System.Linq;
using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Exceptions;
using ALE.ETLBox.src.Definitions.Logging;
using ALE.ETLBox.src.Toolbox.ControlFlow;
using ExcelDataReader.Log.Logger;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ALE.ETLBox.src.Toolbox.ControlFlow
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

        /// <summary>
        /// Фабрика для создания логгера
        /// </summary>
        public static ILoggerFactory LoggerFactory { get; set; } = NullLoggerFactory.Instance;

        public static ILogger GetLogger<T>()
            => LoggerFactory.CreateLogger<T>();

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

        public static void Trace<T>(this ILogger logger, params object[] args)
            => logger.Log<T>(LogLevel.Trace, args);

        public static void Debug<T>(this ILogger logger, params object[] args)
            => logger.Log<T>(LogLevel.Debug, args);

        public static void Info<T>(this ILogger logger, params object[] args)
            => logger.Log<T>(LogLevel.Information, args);

        public static void Warn<T>(this ILogger logger, params object[] args)
            => logger.Log<T>(LogLevel.Warning, args);

        public static void Error<T>(this ILogger logger, params object[] args)
            => logger.Log<T>(LogLevel.Error, args);

        private static void Log<T>(this ILogger logger, LogLevel logLevel, params object[] args)
        {
            var infoFormat = string.Join(" ", args.Select((a, i) => $"{i}"));
            logger.Log(logLevel, infoFormat, args);
        }
    }
}
