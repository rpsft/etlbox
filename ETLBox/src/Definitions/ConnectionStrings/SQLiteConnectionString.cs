using System.Data.Common;
using System.Data.SQLite;

namespace ALE.ETLBox {
    /// <summary>
    /// A helper class for encapsulating a conection string in an object.
    /// Internally the SqlConnectionStringBuilder is used to access the values of the given connection string.
    /// </summary>
    public class SQLiteConnectionString : IDbConnectionString{

        SQLiteConnectionStringBuilder _builder;
        public string Value {
            get {
                return _builder?.ConnectionString;
            }
            set {
                _builder = new SQLiteConnectionStringBuilder(value);
            }
        }

        public SQLiteConnectionStringBuilder OdbcConnectionStringBuilder => _builder;

        public SQLiteConnectionString() {
            _builder = new SQLiteConnectionStringBuilder();
        }

        public SQLiteConnectionString(string connectionString) {
            this.Value = connectionString;
        }


        public static implicit operator SQLiteConnectionString(string v) {
            return new SQLiteConnectionString(v);
        }

        public override string ToString() {
            return Value;
        }
    }
}
