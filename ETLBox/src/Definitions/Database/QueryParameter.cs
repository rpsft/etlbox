﻿using ALE.ETLBox.ConnectionManager;
using ETLBox.Primitives;

namespace ALE.ETLBox
{
    [PublicAPI]
    public class QueryParameter : IQueryParameter
    {
        public string Name { get; }
        public string Type { get; }
        public object Value { get; }

        public DbType DBType => DataTypeConverter.GetDBType(Type);

        public QueryParameter(string name, string type, object value)
        {
            Name = name;
            Type = type;
            Value = value ?? DBNull.Value;
        }
    }
}
