// for string extensions
using ALE.ETLBox.Helper;
using Microsoft.Data.SqlClient;

namespace ALE.ETLBox
{
    /// <summary>
    /// A helper class for encapsulating a conection string to a sql server in an object.
    /// Internally the SqlConnectionStringBuilder is used to access the values of the given connection string.
    /// </summary>
    public class SqlConnectionString : IDbConnectionString
    {

        SqlConnectionStringBuilder _builder;

        public string Value
        {
            get
            {
                return _builder?.ConnectionString.ReplaceIgnoreCase("Integrated Security=true", "Integrated Security=SSPI");

            }
            set
            {

                _builder = new SqlConnectionStringBuilder(value);
            }
        }

        public string DBName => _builder?.InitialCatalog;

        public SqlConnectionStringBuilder SqlConnectionStringBuilder => _builder;

        public SqlConnectionString()
        {
            _builder = new SqlConnectionStringBuilder();
        }

        public SqlConnectionString(string connectionString)
        {
            this.Value = connectionString;
        }

        public SqlConnectionString GetMasterConnection()
        {
            SqlConnectionStringBuilder con = new SqlConnectionStringBuilder(Value);
            con.InitialCatalog = "master";
            return new SqlConnectionString(con.ConnectionString);
        }

        public SqlConnectionString GetConnectionWithoutCatalog()
        {
            SqlConnectionStringBuilder con = new SqlConnectionStringBuilder(Value);
            con.InitialCatalog = "";
            return new SqlConnectionString(con.ConnectionString);
        }

        public static implicit operator SqlConnectionString(string v)
        {
            return new SqlConnectionString(v);
        }

        public override string ToString()
        {
            return Value;
        }
    }
}
