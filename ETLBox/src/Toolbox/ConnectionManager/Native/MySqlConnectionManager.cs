using MySql.Data.MySqlClient;
using System.Data;
using System.Data.SqlClient;

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
            ;
            //using (SqlBulkCopy bulkCopy = new SqlBulkCopy(DbConnection, SqlBulkCopyOptions.TableLock, null))
            //{
            //    bulkCopy.BulkCopyTimeout = 0;
            //    bulkCopy.DestinationTableName = tableName;
            //    foreach (IColumnMapping colMap in data.ColumnMapping)
            //        bulkCopy.ColumnMappings.Add(colMap.SourceColumn, colMap.DataSetColumn);
            //    bulkCopy.WriteToServer(data);
            //}
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
