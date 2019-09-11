using System.Collections.Generic;
using System.Data;

namespace ALE.ETLBox.ConnectionManager {
    public class ConnectionManagerTypeFinder
    {
        public static ConnectionManagerType GetType(IConnectionManager connection)
        {
            if (connection.GetType() == typeof(SqlConnectionManager) ||
                        connection.GetType() == typeof(SMOConnectionManager) ||
                        connection.GetType() == typeof(SqlOdbcConnectionManager)
                        )
                return ConnectionManagerType.SqlServer;
            else if (connection.GetType() == typeof(AccessOdbcConnectionManager))
                return ConnectionManagerType.Access;
            else if (connection.GetType() == typeof(AdomdConnectionManager))
                return ConnectionManagerType.Adomd;
            else if (connection.GetType() == typeof(SQLiteConnectionManager))
                return ConnectionManagerType.SQLLite;
            else return ConnectionManagerType.Unknown;
        }
        }
}
