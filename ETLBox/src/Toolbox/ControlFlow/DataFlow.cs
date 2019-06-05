using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NLog;
using NLog.Extensions.Logging;
using System;
using System.Linq;
using CF = ALE.ETLBox.ControlFlow;

namespace ALE.ETLBox.DataFlow
{
    /// <summary>
    /// Contains static information which affects all Dataflow tasks in ETLBox.
    /// Here you can set a default database connection for the source and for destination components.
    /// If no connection is set here, the default connection provided in the ControlFlow task is used.
    /// </summary>
    public static class DataFlow
    {
        private static IDbConnectionManager sourceDbConnection;
        private static IDbConnectionManager destinationDbConnection;

        public static IDbConnectionManager SourceDbConnection
        {
            get => sourceDbConnection ?? CF.ControlFlow.CurrentDbConnection;
            set => sourceDbConnection = value;
        }
        public static IDbConnectionManager DestinationDbConnection
        {
            get => destinationDbConnection ?? CF.ControlFlow.CurrentDbConnection;
            set => destinationDbConnection = value;
        }

        public static int? LoggingThresholdRows { get; set; } = 1000;
        public static bool HasLoggingThresholdRows => LoggingThresholdRows != null && LoggingThresholdRows > 0;
        /// <summary>
        /// Set all settings back to default (which is null or false)
        /// </summary>
        public static void ClearSettings()
        {
            SourceDbConnection = null;
            DestinationDbConnection = null;
            LoggingThresholdRows = null;
        }

    }



}
