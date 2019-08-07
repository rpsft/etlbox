using System.Collections.Generic;
using System.Data;

namespace ALE.ETLBox.ConnectionManager {
    public enum ConnectionManagerType
    {
        Unknown,
        SqlServer,
        Odbc,
        Adomd,
        SQLLite,
        Access
    }
}
