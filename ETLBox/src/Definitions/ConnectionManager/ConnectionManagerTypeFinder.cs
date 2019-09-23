using System.Collections.Generic;
using System.Data;

namespace ALE.ETLBox.ConnectionManager
{
    public class ConnectionManagerSpecifics
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
                return ConnectionManagerType.SQLite;
            else if (connection.GetType() == typeof(MySqlConnectionManager))
                return ConnectionManagerType.MySql;
            else if (connection.GetType() == typeof(PostgresConnectionManager))
                return ConnectionManagerType.Postgres;
            else return ConnectionManagerType.Unknown;
        }

        public static string GetBeginQuotation(ConnectionManagerType type)
        {
            if (type == ConnectionManagerType.SqlServer)
                return @"[";
            else if (type == ConnectionManagerType.MySql)
                return @"`";
            else if (type == ConnectionManagerType.Postgres)
                return @"""";
            else
                return string.Empty;
        }

        public static string GetEndQuotation(ConnectionManagerType type)
        {
            if (type == ConnectionManagerType.SqlServer)
                return @"]";
            else
                return GetBeginQuotation(type);
        }

        public static string QB(ConnectionManagerType type) => GetBeginQuotation(type);
        public static string QE(ConnectionManagerType type) => GetEndQuotation(type);


    }
}
