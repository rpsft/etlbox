using System;
using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Logging;
using ALE.ETLBox.src.Toolbox.ControlFlow;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;
using NLog.Config;
using NLog.Extensions.Logging;

namespace EtlBox.Logging.Database
{
    [PublicAPI]
    public static class DatabaseLoggingConfiguration
    {
        public const string DefaultLogTableName = "etlbox_log";

        /// <summary>
        /// TableName of the current log process logging table
        /// </summary>
        public static string LogTable { get; set; } = DefaultLogTableName;

        /// <summary>
        /// If you used the logging task StartLoadProces (and created the corresponding load process table before)
        /// then this Property will hold the current load process information.
        /// </summary>
        public static LoadProcess CurrentLoadProcess { get; internal set; }

        public const string DefaultLoadProcessTableName = "etlbox_loadprocess";

        public static void AddDatabaseLoggingConfiguration(IConnectionManager connection) =>
            AddDatabaseLoggingConfiguration(connection, LogLevel.Information, LogTable);

        public static void AddDatabaseLoggingConfiguration(
            IConnectionManager connectionManager,
            LogLevel minLogLevel,
            string tableName)
        {
            if (LogTable != null
                && LogTable != DefaultLoadProcessTableName
                && tableName == DefaultLoadProcessTableName)
            {
                tableName = LogTable;
            }

            ControlFlow.LoggerFactory = LoggerFactory.Create(builder =>
            {
                var newTarget = new CreateDatabaseTarget(
                    connectionManager,
                    tableName
                ).GetNLogDatabaseTarget();

                var config = new LoggingConfiguration();

                config.AddRule(Map(LogLevel.Information), NLog.LogLevel.Error, newTarget);
                builder
                    .ClearProviders()
                    .AddNLog(config);
            });
        }

        private static NLog.LogLevel Map(LogLevel logLevel)
            => logLevel switch
            {
                LogLevel.Trace => NLog.LogLevel.Trace,
                LogLevel.Debug => NLog.LogLevel.Debug,
                LogLevel.Information => NLog.LogLevel.Info,
                LogLevel.Warning => NLog.LogLevel.Warn,
                LogLevel.Critical => NLog.LogLevel.Error,
                LogLevel.Error => NLog.LogLevel.Fatal,
                _ => throw new NotSupportedException($"LogLevel '{logLevel}' is not supported")
            };
    }
}
