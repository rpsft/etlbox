
// for string extensions
using MySql.Data.MySqlClient;

namespace ALE.ETLBox
{
    /// <summary>
    /// A helper class for encapsulating a conection string to a MySql server in an object.
    /// Internally the MySqlConnectionStringBuilder is used to access the values of the given connection string.
    /// </summary>
    public class MySqlConnectionString : IDbConnectionString
    {

        MySqlConnectionStringBuilder _builder;

        public string Value
        {
            get
            {
                return _builder?.ConnectionString;
            }
            set
            {
                _builder = new MySqlConnectionStringBuilder(value);
            }
        }

        public string DBName => _builder?.Database;

        public MySqlConnectionStringBuilder MySqlConnectionStringBuilder => _builder;

        public MySqlConnectionString()
        {
            _builder = new MySqlConnectionStringBuilder();
        }

        public MySqlConnectionString(string connectionString)
        {
            this.Value = connectionString;
        }

        public MySqlConnectionString GetMasterConnection()
        {
            MySqlConnectionStringBuilder con = new MySqlConnectionStringBuilder(Value);
            con.Database = "mysql";
            return new MySqlConnectionString(con.ConnectionString);
        }

        public MySqlConnectionString GetConnectionWithoutCatalog()
        {
            MySqlConnectionStringBuilder con = new MySqlConnectionStringBuilder(Value);
            con.Database = "";
            return new MySqlConnectionString(con.ConnectionString);
        }

        public static implicit operator MySqlConnectionString(string v)
        {
            return new MySqlConnectionString(v);
        }

        public override string ToString()
        {
            return Value;
        }
    }
}
