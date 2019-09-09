using System.Data;
using System.Data.Odbc;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System;

namespace ALE.ETLBox.ConnectionManager {
    /// <summary>
    /// Connection manager for an ODBC connection based on ADO.NET. ODBC can be used to connect to any ODBC able endpoint.
    /// ODBC by default does not support a Bulk Insert - inserting big amounts of data is translated into a
    /// <code>
    /// insert into (...) values (..),(..),(..) statementes.
    /// </code>
    /// Be careful with the batch size - some databases have limitations regarding the length of sql statements.
    /// Reduce the batch if encounter issues here.
    /// </summary>
    public abstract class OdbcConnectionManager : DbConnectionManager<OdbcConnection, OdbcCommand> {
        public OdbcConnectionManager() : base() { }

        public OdbcConnectionManager(OdbcConnectionString connectionString) : base(connectionString) { }

        public override void BulkInsert(ITableData data, string tableName) {
            BulkInsertString bulkInsert = new BulkInsertString() { };
            string sql = bulkInsert.CreateBulkInsertStatement(data, tableName);
            //List<OdbcParameter> parameter = new List<OdbcParameter>();
            //string sql = bulkInsert.CreateBulkInsertStatementWithParameter(data, tableName, ref parameter);
            /*
            string sql = "INSERT INTO CSVDestination (Col1, Col2, Col3, Col4) VALUES (?,?,?,?)";
            var cmd = DbConnection.CreateCommand();
            cmd.Parameters.AddWithValue("name1","1");
            cmd.Parameters.AddWithValue("name1", "2");
            cmd.Parameters.AddWithValue("name1", 3);
            cmd.Parameters.AddWithValue("name1", DBNull.Value);
            */
            var cmd = DbConnection.CreateCommand();
            //cmd.Parameters.AddRange(parameter.ToArray());
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        public override void BeforeBulkInsert() { }
        public override void AfterBulkInsert() { }

    }
}
