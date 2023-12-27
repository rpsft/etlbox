using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;

namespace TestShared.Helper
{
    public class SQLiteConnectionDetails : ConnectionDetails<
        SQLiteConnectionString,
        SQLiteConnectionManager
    >
    {
        private string ConnectionId { get; } = Guid.NewGuid().ToString();
        public SQLiteConnectionDetails(string connectionStringName) : base(connectionStringName)
        {
        }

        public new SQLiteConnectionString ConnectionString(string section)
        {
            var connectionString = new SQLiteConnectionString()
            {
                Value = RawConnectionString(section)
            };
            connectionString.DbName += $"_{ConnectionId}";
            return connectionString;
        }
        
        public new SQLiteConnectionManager ConnectionManager(string section)
        {
            return new SQLiteConnectionManager
            {
                ConnectionString = ConnectionString(section)
            };
        }
    }
}
