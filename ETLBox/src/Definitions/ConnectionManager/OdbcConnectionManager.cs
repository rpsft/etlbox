using System.Data.Odbc;
using ALE.ETLBox.src.Definitions.ConnectionStrings;
using ALE.ETLBox.src.Definitions.Database;

namespace ALE.ETLBox.src.Definitions.ConnectionManager
{
    public abstract class OdbcConnectionManager : DbConnectionManager<OdbcConnection>
    {
        protected OdbcConnectionManager() { }

        protected OdbcConnectionManager(OdbcConnectionString connectionString)
            : base(connectionString) { }

        internal void OdbcBulkInsert(ITableData data, string tableName, BulkInsertSql bulkInsert)
        {
            var sql = bulkInsert.CreateBulkInsertStatement(data, tableName);
            var cmd = DbConnection.CreateCommand();
            cmd.Transaction = Transaction as OdbcTransaction;
            cmd.Parameters.AddRange(bulkInsert.Parameters.ToArray());
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
    }
}
