using System.Globalization;
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
        public override ConnectionManagerType ConnectionManagerType { get; } =
            ConnectionManagerType.MySql;
        public override string QB { get; } = @"`";
        public override string QE { get; } = @"`";
        public override CultureInfo ConnectionCulture => CultureInfo.InvariantCulture;
        public override bool SupportSchemas { get; } = false;

        public MySqlConnectionManager() { }

        public MySqlConnectionManager(MySqlConnectionString connectionString)
            : base(connectionString) { }

        public MySqlConnectionManager(string connectionString)
            : base(new MySqlConnectionString(connectionString)) { }

        public override void BulkInsert(ITableData data, string tableName)
        {
            BulkInsertSql<MySqlParameter> bulkInsert = new BulkInsertSql<MySqlParameter>
            {
                ConnectionType = ConnectionManagerType.MySql,
                QB = QB,
                QE = QE,
                UseParameterQuery = true,
            };
            string sql = bulkInsert.CreateBulkInsertStatement(data, tableName);
            var cmd = DbConnection.CreateCommand();
            cmd.Transaction = Transaction as MySqlTransaction;
            cmd.Parameters.AddRange(bulkInsert.Parameters.ToArray());
            cmd.CommandText = sql;
            cmd.Prepare();
            cmd.ExecuteNonQuery();
        }

        public override void PrepareBulkInsert(string tablename) { }

        public override void CleanUpBulkInsert(string tablename) { }

        public override void BeforeBulkInsert(string tableName) { }

        public override void AfterBulkInsert(string tableName) { }

        public override IConnectionManager Clone()
        {
            MySqlConnectionManager clone = new MySqlConnectionManager(
                (MySqlConnectionString)ConnectionString
            )
            {
                MaxLoginAttempts = MaxLoginAttempts
            };
            return clone;
        }
    }
}
