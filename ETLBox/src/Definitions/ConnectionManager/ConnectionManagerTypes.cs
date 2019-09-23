using System.Collections.Generic;
using System.Data;

namespace ALE.ETLBox.ConnectionManager {
    public enum ConnectionManagerType
    {
        Unknown,
        SqlServer,
        Adomd,
        SQLite,
        MySql,
        Postgres,
        Access
    }
}
