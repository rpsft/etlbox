using System.Data;
using System.Globalization;
using Microsoft.Data.SqlClient;

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
    [PublicAPI]
    public class SqlConnectionManager : DbConnectionManager<SqlConnection>
    {
        public override ConnectionManagerType ConnectionManagerType { get; } =
            ConnectionManagerType.SqlServer;
        public override string QB { get; } = @"[";
        public override string QE { get; } = @"]";
        public override CultureInfo ConnectionCulture => CultureInfo.CurrentCulture;

        public bool ModifyDBSettings { get; set; }

        public SqlConnectionManager() { }

        public SqlConnectionManager(SqlConnectionString connectionString)
            : base(connectionString) { }

        public SqlConnectionManager(string connectionString)
            : base(new SqlConnectionString(connectionString)) { }

        private string PageVerify { get; set; }
        private string RecoveryModel { get; set; }

        public override void BulkInsert(ITableData data, string tableName)
        {
            using (
                SqlBulkCopy bulkCopy = new SqlBulkCopy(
                    DbConnection,
                    SqlBulkCopyOptions.TableLock,
                    Transaction as SqlTransaction
                )
            )
            {
                bulkCopy.BulkCopyTimeout = 0;
                bulkCopy.DestinationTableName = tableName;
                foreach (IColumnMapping colMap in data.ColumnMapping)
                    bulkCopy.ColumnMappings.Add(colMap.SourceColumn, colMap.DataSetColumn);
                bulkCopy.WriteToServer(data);
            }
        }

        public override void PrepareBulkInsert(string tablename)
        {
            if (ModifyDBSettings)
            {
                try
                {
                    string dbName = DbConnection.Database;
                    PageVerify = ExecuteScalar(
                            $"SELECT page_verify_option_desc FROM sys.databases WHERE NAME = '{dbName}'"
                        )
                        .ToString();
                    RecoveryModel = ExecuteScalar(
                            $"SELECT recovery_model_desc FROM sys.databases WHERE NAME = '{dbName}'"
                        )
                        .ToString();
                    ExecuteNonQuery(@"USE master");
                    ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET PAGE_VERIFY NONE;");
                    ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET RECOVERY BULK_LOGGED");
                    ExecuteNonQuery($@"USE [{dbName}]");
                }
                catch
                {
                    ModifyDBSettings = false;
                }
            }
        }

        public override void BeforeBulkInsert(string tableName) { }

        public override void AfterBulkInsert(string tableName) { }

        public override void CleanUpBulkInsert(string tablename)
        {
            if (ModifyDBSettings)
            {
                try
                {
                    string dbName = DbConnection.Database;
                    ExecuteNonQuery(@"USE master");
                    ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET PAGE_VERIFY {PageVerify};");
                    ExecuteNonQuery($@"ALTER DATABASE [{dbName}] SET RECOVERY {RecoveryModel}");
                    ExecuteNonQuery($@"USE [{dbName}]");
                }
                catch
                {
                    // ignored
                }
            }
        }

        public override IConnectionManager Clone()
        {
            SqlConnectionManager clone = new SqlConnectionManager(
                (SqlConnectionString)ConnectionString
            )
            {
                MaxLoginAttempts = MaxLoginAttempts,
                ModifyDBSettings = ModifyDBSettings
            };
            return clone;
        }
    }
}
