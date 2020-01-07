using MySql.Data.MySqlClient;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Connection manager of a classic ADO.NET connection to a (Microsoft) Sql Server.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.DefaultDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;"));
    /// </code>
    /// </example>
    public class MySqlConnectionManager : DbConnectionManager<MySqlConnection>
    {
        public MySqlConnectionManager() : base() { }

        public MySqlConnectionManager(MySqlConnectionString connectionString) : base(connectionString) { }

        public MySqlConnectionManager(string connectionString) : base(new MySqlConnectionString(connectionString)) { }

        string PageVerify { get; set; }
        string RecoveryModel { get; set; }
        public override void BulkInsert(ITableData data, string tableName)
        {
            BulkInsertSql<MySqlParameter> bulkInsert = new BulkInsertSql<MySqlParameter>()
            {
                UseParameterQuery = true,
                ConnectionType = ConnectionManagerType.MySql
            };
            string sql = bulkInsert.CreateBulkInsertStatement(data, tableName);
            var cmd = DbConnection.CreateCommand();
            cmd.Parameters.AddRange(bulkInsert.Parameters.ToArray());
            cmd.CommandText = sql;
            cmd.Prepare();
            cmd.ExecuteNonQuery();
        }

        public override void BeforeBulkInsert(string tableName) { }

        public override void AfterBulkInsert(string tableName) { }

        public override IConnectionManager Clone()
        {
            MySqlConnectionManager clone = new MySqlConnectionManager((MySqlConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
