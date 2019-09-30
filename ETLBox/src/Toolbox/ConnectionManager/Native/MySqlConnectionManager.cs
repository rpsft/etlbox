using CsvHelper;
using MySql.Data.MySqlClient;
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
    public class MySqlConnectionManager : DbConnectionManager<MySqlConnection>
    {
        public bool ModifyDBSettings { get; set; } = true;

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
            //            var records = new List<object>
            //    {
            //        new { Id = 1, Name = "one" },
            //    };
            //            using (var writer = new StreamWriter(".etlboxtemp.csvdata"))
            //            using (var csv = new CsvWriter(writer))
            //            {
            //                csv.WriteRecords(records);
            //            }

            //            string sql = $@"LOAD DATA INFILE '.etlboxtemp.csvdata' 
            //INTO TABLE {tableName}
            //FIELDS TERMINATED BY ';' 
            //ENCLOSED BY '""'
            //LINES TERMINATED BY '\r\n'
            //IGNORE 1 LINES; "
            //            ;
            //            var cmd = DbConnection.CreateCommand();
            //            cmd.CommandText = sql;
            //            cmd.ExecuteNonQuery();
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
            MySqlConnectionManager clone = new MySqlConnectionManager((MySqlConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
