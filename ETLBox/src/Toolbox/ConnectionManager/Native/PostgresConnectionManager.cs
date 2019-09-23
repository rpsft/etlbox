using CsvHelper;
using MySql.Data.MySqlClient;
using Npgsql;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Connection manager of a classic ADO.NET connection to a (Microsoft) Sql Server.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;"));
    /// </code>
    /// </example>
    public class PostgresConnectionManager : DbConnectionManager<NpgsqlConnection>
    {
        public bool ModifyDBSettings { get; set; } = true;

        public PostgresConnectionManager() : base() { }

        public PostgresConnectionManager(PostgresConnectionString connectionString) : base(connectionString) { }

        public PostgresConnectionManager(string connectionString) : base(new PostgresConnectionString(connectionString)) { }

        string PageVerify { get; set; }
        string RecoveryModel { get; set; }
        public override void BulkInsert(ITableData data, string tableName)
        {
            //BulkInsertSql<MySqlParameter> bulkInsert = new BulkInsertSql<MySqlParameter>()
            //{
            //    UseParameterQuery = true
            //};
            //string sql = bulkInsert.CreateBulkInsertStatement(data, tableName);
            //var cmd = DbConnection.CreateCommand();
            //cmd.Parameters.AddRange(bulkInsert.Parameters.ToArray());
            //cmd.CommandText = sql;
            //cmd.ExecuteNonQuery();

        }

        public override void BeforeBulkInsert()
        {
            if (ModifyDBSettings)
            {
                ;
            }
        }

        public override void AfterBulkInsert()
        {
            if (ModifyDBSettings)
            {
                ;
            }
        }

        public override IConnectionManager Clone()
        {
            PostgresConnectionManager clone = new PostgresConnectionManager((PostgresConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
