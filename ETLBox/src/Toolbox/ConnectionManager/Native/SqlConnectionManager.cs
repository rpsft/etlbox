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
    public class SqlConnectionManager : DbConnectionManager<SqlConnection>
    {
        public bool ModifyDBSettings { get; set; } = false;

        public SqlConnectionManager() : base() { }

        public SqlConnectionManager(ConnectionString connectionString) : base(connectionString) { }

        public SqlConnectionManager(string connectionString) : base(new ConnectionString(connectionString)) { }

        string PageVerify { get; set; }
        string RecoveryModel { get; set; }
        public override void BulkInsert(ITableData data, string tableName)
        {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(DbConnection, SqlBulkCopyOptions.TableLock, null))
            {
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.DestinationTableName = tableName;
                foreach (IColumnMapping colMap in data.ColumnMapping)
                    bulkCopy.ColumnMappings.Add(colMap.SourceColumn, colMap.DataSetColumn);
                bulkCopy.WriteToServer(data);
            }
        }

        public override void BeforeBulkInsert(string tableName)
        {
            if (ModifyDBSettings)
            {
                try
                {
                    string dbName = this.DbConnection.Database;
                    PageVerify = this.ExecuteScalar($"SELECT page_verify_option_desc FROM sys.databases WHERE NAME = '{dbName}'").ToString();
                    RecoveryModel = this.ExecuteScalar($"SELECT recovery_model_desc FROM sys.databases WHERE NAME = '{dbName}'").ToString();
                    this.ExecuteNonQuery($@"USE master");
                    this.ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET PAGE_VERIFY NONE;");
                    this.ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET RECOVERY BULK_LOGGED");
                    this.ExecuteNonQuery($@"USE [{dbName}]");
                }
                catch
                {
                    ModifyDBSettings = false;
                }
            }
        }

        public override void AfterBulkInsert(string tableName)
        {
            if (ModifyDBSettings)
            {
                try {
                string dbName = this.DbConnection.Database;
                this.ExecuteNonQuery($@"USE master");
                this.ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET PAGE_VERIFY {PageVerify};");
                this.ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET RECOVERY {RecoveryModel}");
                this.ExecuteNonQuery($@"USE [{dbName}]");
                }
                catch {  }
            }
        }

        public override IConnectionManager Clone()
        {
            SqlConnectionManager clone = new SqlConnectionManager((ConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
