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
    /// Here you can set the threshold value when information about processed records should appear.
    /// </summary>
    public static class DataFlow
    {
        public static int? LoggingThresholdRows { get; set; } = 1000;
        public static bool HasLoggingThresholdRows => LoggingThresholdRows != null && LoggingThresholdRows > 0;
        /// <summary>
        /// Set all settings back to default (which is null or false)
        /// </summary>
        public static void ClearSettings()
        {
            LoggingThresholdRows = 1000;
        }
    }
}
