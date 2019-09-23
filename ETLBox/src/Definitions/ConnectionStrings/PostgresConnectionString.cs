using System.Data.SqlClient;
using ALE.ETLBox.Helper;
using MySql.Data.MySqlClient;
using Npgsql;

namespace ALE.ETLBox {
    /// <summary>
    /// A helper class for encapsulating a conection string to a MySql server in an object.
    /// Internally the MySqlConnectionStringBuilder is used to access the values of the given connection string.
    /// </summary>
    public class PostgresConnectionString : IDbConnectionString {

        NpgsqlConnectionStringBuilder _builder;

        public string Value {
            get {
                return _builder?.ConnectionString;
            }
            set {
                _builder = new NpgsqlConnectionStringBuilder(value);
            }
        }

        public string DBName => _builder?.Database;

        public NpgsqlConnectionStringBuilder MySqlConnectionStringBuilder => _builder;

        public PostgresConnectionString() {
            _builder = new NpgsqlConnectionStringBuilder();
        }

        public PostgresConnectionString(string connectionString) {
            this.Value = connectionString;
        }

        public PostgresConnectionString GetMasterConnection() {
            NpgsqlConnectionStringBuilder con = new NpgsqlConnectionStringBuilder(Value);
            con.Database = "postgres";
            return new PostgresConnectionString(con.ConnectionString);
        }

        public PostgresConnectionString GetConnectionWithoutCatalog() {
            MySqlConnectionStringBuilder con = new MySqlConnectionStringBuilder(Value);
            con.Database = "";
            return new PostgresConnectionString(con.ConnectionString);
        }

        public static implicit operator PostgresConnectionString(string v) {
            return new PostgresConnectionString(v);
        }

        public override string ToString() {
            return Value;
        }
    }
}
