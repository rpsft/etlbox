using System.Data;
using System.Data.Odbc;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System;

namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Abstract implementation of Connection manager for an ODBC connection based on ADO.NET.
    /// ODBC can be used to connect to any ODBC able endpoint. Please use the specific ODBC
    /// for each database, e.g. use the SqlOdbcConnectionManager when connecting to Sql Server via Odbc or
    /// the AccessOdbcConnectionManager when connecting to an Access database via Odbc.
    /// Please note: ODBC by default does not support a Bulk Insert -
    /// inserting big amounts of data is translated into a
    /// <code>
    /// insert into (...) values (..),(..),(..) statementes.
    /// </code>
    /// This means that inserting big amounts of data in a database via Odbc can be much slower
    /// than using the native connector.
    /// Also be careful with the batch size - some databases have limitations regarding the length of sql statements.
    /// Reduce the batch size if you encounter issues here.
    /// </summary>
    public abstract class OdbcConnectionManager : DbConnectionManager<OdbcConnection, OdbcCommand>
    {
        public OdbcConnectionManager() : base() { }

        public OdbcConnectionManager(OdbcConnectionString connectionString) : base(connectionString) { }

        public override void BulkInsert(ITableData data, string tableName)
        {
            BulkInsertSql bulkInsert = new BulkInsertSql()
            {
                UseParameterQuery = true
            };
            string sql = bulkInsert.CreateBulkInsertStatement(data, tableName);
            var cmd = DbConnection.CreateCommand();
            cmd.Parameters.AddRange(bulkInsert.Parameters.ToArray());
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public override void BeforeBulkInsert() { }
        public override void AfterBulkInsert() { }

    }
}
