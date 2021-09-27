using ETLBox.Helper;
using System;
using System.Data;

namespace ETLBox.ControlFlow
{
    /// <summary>
    /// A parameter used in a query
    /// </summary>
    public class QueryParameter
    {
        /// <summary>
        /// Name of the parameter
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The sql parameter type (e.g. "INT")
        /// </summary>
        public string Type { get; set; }

        /// <summary>
        /// The value of the parameter
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// The database type parsed from the parameter type
        /// </summary>
        public DbType? DBType => DataTypeConverter.GetDBType(Type);
        public int DBSize => DataTypeConverter.GetStringLengthFromCharString(Type);

        public QueryParameter() {

        }

        public QueryParameter(object value) :this() {
            Value = value ?? DBNull.Value;
        }

        public QueryParameter(string name, string type, object value) : this(value){
            Name = name;
            Type = type;            
        }
    }
}
