using System.Data.Common;
using System.Data.Odbc;

namespace ALE.ETLBox {
    /// <summary>
    /// A helper class for encapsulating a conection string in an object.
    /// Internally the OdbcConnectionStringBuilder is used to access the values of the given connection string.
    /// </summary>
    public class OdbcConnectionString : IDbConnectionString{

        OdbcConnectionStringBuilder _builder;
        public string Value {
            get {
                return _builder?.ConnectionString;
            }
            set {
                _builder = new OdbcConnectionStringBuilder(value);
            }
        }

        public OdbcConnectionStringBuilder OdbcConnectionStringBuilder => _builder;

        public OdbcConnectionString() {
            _builder = new OdbcConnectionStringBuilder();
        }

        public OdbcConnectionString(string connectionString) {
            this.Value = connectionString;
        }


        public static implicit operator OdbcConnectionString(string v) {
            return new OdbcConnectionString(v);
        }

        public override string ToString() {
            return Value;
        }
    }
}
