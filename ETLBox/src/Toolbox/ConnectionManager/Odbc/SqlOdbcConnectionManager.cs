namespace ALE.ETLBox.ConnectionManager
{
    /// <summary>
    /// Sql Connection manager for an ODBC connection based on ADO.NET to Sql Server.
    /// ODBC by default does not support a Bulk Insert - inserting big amounts of data is translated into a
    /// <code>
    /// insert into (...) values (..),(..),(..) statementes.
    /// </code>
    /// This means that inserting big amounts of data in a database via Odbc can be much slower
    /// than using the native connector.
    /// Also be careful with the batch size - some databases have limitations regarding the length of sql statements.
    /// Reduce the batch size if you encounter issues here.
    /// </summary>
    /// <example>
    /// <code>
    /// ControlFlow.CurrentDbConnection =
    ///   new OdbcConnectionManager(new ObdcConnectionString(
    ///     "Driver={SQL Server};Server=.;Database=ETLBox;Trusted_Connection=Yes;"));
    /// </code>
    /// </example>
    public class SqlOdbcConnectionManager : OdbcConnectionManager
    {
        public SqlOdbcConnectionManager() : base() { }

        public SqlOdbcConnectionManager(OdbcConnectionString connectionString) : base(connectionString) { }
        public SqlOdbcConnectionManager(string connectionString) : base(new OdbcConnectionString(connectionString)) { }

        public override IConnectionManager Clone()
        {
            if (LeaveOpen) return this;
            SqlOdbcConnectionManager clone = new SqlOdbcConnectionManager((OdbcConnectionString)ConnectionString)
            {
                MaxLoginAttempts = this.MaxLoginAttempts
            };
            return clone;
        }
    }
}
