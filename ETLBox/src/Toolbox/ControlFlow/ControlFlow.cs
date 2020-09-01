using ETLBox.Connection;
using ETLBox.Exceptions;
using NLog;

namespace ETLBox.ControlFlow
{
    /// <summary>
    /// Contains static information which affects all ETLBox tasks and general logging behavior for all components.
    /// Here you can set default connections string, disbale the logging for all processes or set the current stage used in your logging configuration.
    /// </summary>
    public static class ControlFlow
    {
        private static IConnectionManager _defaultDbConnection;
        /// <summary>
        /// You can store your general database connection string here. This connection will then used by all Tasks where no DB connection is excplicitly set.
        /// </summary>
        public static IConnectionManager DefaultDbConnection
        {
            get
            {
                if (_defaultDbConnection == null)
                    throw new ETLBoxException("No connection manager found! The component or task you are " +
                        "using expected a  connection manager to connect to the database." +
                        "Either pass a connection manager or set a default connection manager within the " +
                        "ControlFlow.DefaultDbConnection property!");
                return _defaultDbConnection;
            }
            set
            {
                _defaultDbConnection = value;
            }
        }

        /// <summary>
        /// Set all settings back to default (which is null or false)
        /// </summary>
        public static void ClearSettings()
        {
            DefaultDbConnection = null;
        }
    }
}
