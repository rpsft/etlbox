using ALE.ETLBox.ConnectionManager;
using System;
using System.Data;

namespace ALE.ETLBox
{
    public class QueryParameter
    {
        public string Name { get; set; }
        public string Type { get; set; }
        public object Value { get; set; }

        public DbType DBType => DataTypeConverter.GetDBType(Type);

        public QueryParameter(string name, string type, object value)
        {
            Name = name;
            Type = type;
            Value = value ?? DBNull.Value;
        }
    }
}
