using Microsoft.Extensions.Logging;
using System;

namespace ETLBox.Logging
{
    /// <summary>
    /// Contains static information which affects all ETLBox tasks and general logging behavior for all components.
    /// Here you can set default connections string, disable the logging for all processes or set the current stage used in your logging configuration.
    /// </summary>
    public static class Logging
    {
        /// <summary>
        /// This is the default value for all control and dataflow components. If set to true, no log messages will be produced.
        /// Logging can be enabled/disabled for all components individually using the DisableLogging property on each component. 
        /// </summary>
        public static bool DisableAllLogging { get; set; }

        /// <summary>
        /// For logging purposes only. If the stage is set, you can access the stage value in the logging configuration.
        /// </summary>
        [Obsolete]
        public static string STAGE { get; set; }

        /// <summary>
        /// If you used the logging task StartLoadProces (and created the corresponding load process table before)
        /// then this Property will hold the current load process information.
        /// </summary>
        [Obsolete]
        public static LoadProcess CurrentLoadProcess { get; internal set; }

        public const string DEFAULTLOADPROCESSTABLENAME = "etlbox_loadprocess";
        /// <summary>
        /// TableName of the current load process logging table
        /// </summary>
        [Obsolete]
        public static string LoadProcessTable { get; set; } = DEFAULTLOADPROCESSTABLENAME;

        /// <summary>
        /// The default log table name
        /// </summary>
        public const string DEFAULTLOGTABLENAME = "etlbox_log";

        /// <summary>
        /// TableName of the current log process logging table
        /// </summary>
        [Obsolete]
        public static string LogTable { get; set; } = DEFAULTLOGTABLENAME;

        /// <summary>
        /// Set all settings back to default (which is null or false)
        /// </summary>
        public static void ClearSettings() {
            CurrentLoadProcess = null;
            DisableAllLogging = false;
            LoadProcessTable = DEFAULTLOADPROCESSTABLENAME;
            LogTable = DEFAULTLOGTABLENAME;
            STAGE = null;
        }

        public static ILogger LogInstance { get; set; }

    }

}
