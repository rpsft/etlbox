// for string extensions
using ALE.ETLBox.Helper;
using System.Data.SqlClient;

namespace ALE.ETLBox
{
    /// <summary>
    /// A helper class for encapsulating a conection string to a sql server in an object.
    /// Internally the SqlConnectionStringBuilder is used to access the values of the given connection string.
    /// </summary>
    public class ConnectionString : IDbConnectionString
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

        public ConnectionString()
        {
            _builder = new SqlConnectionStringBuilder();
        }

        public ConnectionString(string connectionString)
        {
            this.Value = connectionString;
        }

        public ConnectionString GetMasterConnection()
        {
            SqlConnectionStringBuilder con = new SqlConnectionStringBuilder(Value);
            con.InitialCatalog = "master";
            return new ConnectionString(con.ConnectionString);
        }

        public ConnectionString GetConnectionWithoutCatalog()
        {
            SqlConnectionStringBuilder con = new SqlConnectionStringBuilder(Value);
            con.InitialCatalog = "";
            return new ConnectionString(con.ConnectionString);
        }

        public static implicit operator ConnectionString(string v)
        {
            return new ConnectionString(v);
        }

        public override string ToString()
        {
            return Value;
        }
    }
}
