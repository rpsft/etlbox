namespace ALE.ETLBox.ConnectionManager
{
    public class ConnectionManagerSpecifics
    {
        public static string GetBeginQuotation(ConnectionManagerType type)
        {
            if (type == ConnectionManagerType.SqlServer || type == ConnectionManagerType.Access)
                return @"[";
            else if (type == ConnectionManagerType.MySql)
                return @"`";
            else if (type == ConnectionManagerType.Postgres || type == ConnectionManagerType.SQLite)
                return @"""";
            else
                return string.Empty;
        }

        public static string GetEndQuotation(ConnectionManagerType type)
        {
            if (type == ConnectionManagerType.SqlServer || type == ConnectionManagerType.Access)
                return @"]";
            else
                return GetBeginQuotation(type);
        }


        public static string GetBeginQuotation(IConnectionManager connectionManager) => GetBeginQuotation(connectionManager.ConnectionManagerType);
        public static string GetEndQuotation(IConnectionManager connectionManager) => GetEndQuotation(connectionManager.ConnectionManagerType);


    }
}
